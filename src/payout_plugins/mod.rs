//! Payout plugin system for HydraPool.
//!
//! HydraPool's native payout path is on-chain coinbase outputs (max 100
//! payees). Plugins extend payouts to Lightning (BOLT12/BOLT11 invoices)
//! and eCash (Cashu ecash, Fedimint) by accruing a miner's share of each
//! pool-found block into a ledger and paying the accrued balance
//! out-of-band.
//!
//! Flow:
//! 1. `src/main.rs` subscribes directly to bitcoind's `zmqpubhashblock`
//!    (the lib's `ZmqListener` discards the block hash). On EVERY
//!    network block it fetches the block over RPC and gates on the
//!    block actually being a pool block (coinbase ends with the
//!    configured `pool_signature` and the coinbase outputs sum to the
//!    latest template's `coinbasevalue` — see
//!    [`block_found`]). Only a pool-found block accrues.
//! 2. The accrual mirrors the on-chain coinbase builder: donation bips
//!    are cut first, fee bips second, and only the remainder is split
//!    difficulty-weighted across the PPLNS window — so the ledger
//!    accrues exactly what the coinbase paid to miners.
//! 3. All plugins share one [`crate::payout_plugins::ledger::Ledger`] of
//!    accrued per-miner balances (keyed by the miner's identifier —
//!    username, npub, or lightning address as given in the stratum
//!    authorize line), namespaced per plugin.
//! 4. On a fixed 60s tick, the registry signals each rail's dedicated
//!    drain task; the task runs that plugin's `payout_due` (LND REST,
//!    mint REST, federation gateway) and records the payout in the
//!    ledger.
//!
//! ## Parallel drains
//!
//! The tick never runs payouts inline: each enabled rail has exactly
//! one drain task (see [`registry`]), woken by a per-rail
//! [`tokio::sync::Notify`]. A slow rail therefore never delays another
//! rail's tick, and one-task-per-rail guarantees two concurrent drains
//! of the SAME rail never happen. Concurrent drains across DIFFERENT
//! rails are safe: pending intents are per (plugin, miner) in the
//! ledger and each rail only touches its own namespace.
//!
//! Each drain attempt is bounded by a per-rail timeout — default 120s,
//! override per rail with `<prefix>_DRAIN_TIMEOUT_SECS`
//! (`HYDRA_LN_DRAIN_TIMEOUT_SECS`, `HYDRA_CASHU_DRAIN_TIMEOUT_SECS`,
//! `HYDRA_FEDIMINT_DRAIN_TIMEOUT_SECS`). A timed-out attempt cancels
//! the drain future; any payment it already dispatched is bracketed by
//! a pending intent and reconciled, never re-paid. Failed attempts
//! retry with exponential backoff (capped at ~10 minutes), reset on
//! success.
//!
//! ## Circuit breaker
//!
//! The registry tracks consecutive failed drain attempts per rail.
//! After 5 consecutive failures the rail is marked `degraded`:
//! tick-driven drains stop (accrual via `on_block` is unaffected), the
//! rail slow-probes once per backoff cap so it can recover without
//! operator intervention, and an error is logged every 30 ticks with
//! the failure count and last error. A successful drain resets the
//! counter and re-enables tick drains. Per-rail state is exposed for
//! metrics via [`registry::PayoutPluginRegistry::rail_status`].
//!
//! ## Feature flags
//!
//! Each rail is behind a cargo feature — `ln`, `cashu`, `fedimint`
//! (all on by default). Build a single-rail binary with e.g.
//! `cargo check --no-default-features --features ln`; the module
//! declarations here and the registrations in `registry::from_env` are
//! cfg-gated accordingly.
//!
//! ## Crash safety
//!
//! A payment dispatched but not settled is bracketed by pending-intent
//! ledger entries (`begin_payout` / `settle_payout` /
//! `rollback_payout`) so a crash between pay and record can be
//! reconciled on restart instead of re-paying: startup reconciliation
//! (`Ledger::stranded_pending_payouts`) logs every intent still open
//! from a previous run — including its backend `idem_key` when one was
//! captured, so operators can reconcile against the backend by that
//! receipt id — and the cashu plugin re-delivers minted payloads
//! attached to open intents.

pub mod block_found;
#[cfg(feature = "cashu")]
pub mod cashu;
#[cfg(feature = "fedimint")]
pub mod fedimint;
pub mod ledger;
#[cfg(feature = "ln")]
pub mod lightning;
pub mod registry;
pub mod traits;

#[allow(unused_imports)]
pub use traits::{MinerBalance, PayoutPlugin, PluginContext, PluginError};
