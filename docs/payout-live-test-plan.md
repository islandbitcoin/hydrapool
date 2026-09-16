# Payout Rails — Manual Live Test Plan

> Goal: prove all three payout rails end-to-end with real (test-value) payouts
> before opening the upstream PR. Everything runs on **signet** (or Mutinynet)
> so coins are free and block times are controllable.

## Test environment (one machine, Docker)

| Component | Image | Why |
|---|---|---|
| bitcoind (signet) | `bitcoin/bitcoind:28` | chain + `zmqpubhashblock` + RPC; mine blocks on demand with a generatetoaddress wallet |
| hydrapool | built from this branch | pool under test; run on host (`cargo run`) or in compose |
| LND (signet) | `lightninglabs/lnd` | LN rail backend; funded from signet faucet coins |
| CDK mint (signet) | `cashubtc/cdk` | Cashu rail backend; mint funded from same wallet |
| Fedimint gateway + federation | fedimint dev-env | Fedimint rail; heaviest to stand up — see phase order |

Signet faucets: https://signetfaucet.com (global) or run the official
faucet challenge locally. Mutinynet (https://mutinynet.com) is the
friendlier option: 30s blocks, generous faucet — but confirm
`docker/signet` config parity first; hydrapool supports `signet`
natively, so default to signet unless faucet friction forces the swap.

## Phase 0 — Wiring sanity (no payouts)

1. `./container/build.sh`-equivalent: `cargo build --release`; run pool with
   `config-signet.toml` (copy of `docker/signet` config, `network = "signet"`).
2. Connect one real miner (cpuminer or a spare S9 pointing at signet stratum)
   or use the pool's test harness; confirm shares flow.
3. Watch the new log chain on every signet block:
   - `hashblock` received → `getblock` fetch
   - **Non-pool block** → `foreign block, no accrual` (the gate working)
   - Mine a block TO the pool (wallet template) → `pool block, accruing`
   - Ledger lines appear per miner with `rail` set.
4. **Check:** donation cut math — set `donation = 100` (1%), mine a block,
   verify accrued total = coinbasevalue − 1%.

✅ Exit when: gate rejects foreign blocks, accrues ours exactly once, cuts correct.

## Phase 1 — Lightning rail (highest risk, do first)

1. Stand up LND (signet), fund on-chain from faucet, open 1 channel to a
   well-connected signet node (e.g. the faucet's node).
2. Config: `HYDRA_LN_API_URL=https://127.0.0.1:8080`,
   `HYDRA_LN_API_MACAROON=<invoice+pay macaroon hex>`,
   `HYDRA_LN_THRESHOLD_SATS=1000` (low for test).
3. Miner side: set username to a real Lightning Address (get a free one at
   zbd.gg / getalby.com — both work on signet via LNURL; or run LNURLd).
4. Test matrix:
   - Happy path: accrue ≥ threshold → next 60s tick pays → invoice settled,
     `settle_payout` stamped with payment hash, balance zeroed.
   - Wrong invoice amount: run a fake LNURL endpoint returning amount+1000 sats
     → pool must refuse (h-tag/amount verification).
   - Missing metadata: fake endpoint without `metadata` → fail closed.
   - Timeout: stop LND mid-pass → intent stays OPEN (not rolled back);
     restart LND → next pass reconciles, pays once, no double-pay.
   - Circuit breaker: `iptables`-drop LND port ×5 failures → rail logs
     `degraded`, other rails' drains still fire; restore → auto-recovers.
5. **Verify on LND side**: `lncli listpayments` — each ledger payout maps to
   exactly one settled payment.

## Phase 2 — Cashu rail

1. Run a CDK mint (signet Lightning backend or the standalone test mint),
   fund the pool's mint wallet.
2. Config: `HYDRA_CASHU_MINT_URL`, `HYDRA_CASHU_THRESHOLD_SATS=1000`,
   `HYDRA_CASHU_DESTINATIONS={"rig1":"<topic>:<token>"}` with a reserved
   ntfy topic + access token.
3. Test matrix:
   - Happy path: mint → deliver to ntfy → settle only after delivery ack.
   - **Delivery failure**: revoke the topic token after mint → token stays on
     the open intent → restore → next pass re-delivers the SAME token
     (check `mint` endpoint hit count stays 1 — no double-mint).
   - Crash across restart: kill the pool between mint and deliver → on boot,
     `stranded_pending_payouts` logs the intent with idem_key → next pass
     re-delivers.
   - Bare public topic (`hydrapool-rig1`) → config-time refusal.
4. **Verify on wallet side**: redeem the token in a Cashu wallet (e.g.
   Nutstash/test wallet pointing at the mint) — proofs are valid.

## Phase 3 — Fedimint rail

1. Heaviest setup: fedimint dev-env single-guardian federation + gateway.
   If the gateway's `/credit`-shaped endpoint differs from what
   `fedimint.rs` assumes, **fix the plugin against the real API** — this
   phase exists to catch exactly that.
2. Test matrix: happy path (gateway op_id returned → settled), gateway down
   (rollback, retry), gateway timeout mid-credit (intent stays open,
   op_id reconciled on restart).

## Phase 4 — Full soak

1. All three rails enabled, threshold 1000 sats, signet mining scripted:
   1 block every 10 min for ~12h (cron `bitcoin-cli generatetoaddress 1`).
2. Assertions at the end:
   - ledger replay zero stranded intents
   - sum(payouts per rail) == sum(coinbase attributable) ± truncation dust
   - LND/CDK/gateway ledgers match our settle records 1:1 (idem_key join)
   - no rail degraded at finish; fmt/clippy/tests still green.
3. Optional: flip `pool_signature` off and confirm anonymous-mode fallback
   gate (parent-link match) still catches our blocks.

## Known-good sequencing tips

- Mine TO the pool's template: use `bitcoin-cli generatetoaddress` only for
  filler blocks; to make the pool find a block, connect a CPU miner at high
  share difficulty on signet — or temporarily set `difficulty_multiplier`
  so the template's target matches CPU-minable difficulty.
- Do Phase 1 first: LN has the most moving parts and the double-pay class of
  bugs. Phases 2–3 reuse the same intent machinery.
- Snapshot the Docker volumes after each phase — a bad ledger state is
  10× cheaper to restore than to debug.

## Definition of done

All phases green + `ledger`, LND, mint, gateway ledgers reconciled 1:1 →
open the upstream PR referencing this document's results (attach the soak
summary numbers).
