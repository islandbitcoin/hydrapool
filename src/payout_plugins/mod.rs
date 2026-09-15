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
//! 4. On a fixed 60s tick, the registry calls each plugin's
//!    `payout_due`; the plugin pays out via its backend (LND REST,
//!    mint REST, federation gateway) and records the payout in the
//!    ledger.
//!
//! Failures are retried on the next fixed-interval tick (there is no
//! backoff); a payout that cannot complete leaves the balance untouched,
//! never lost. A payment dispatched but not settled is bracketed by
//! pending-intent ledger entries (`begin_payout` / `settle_payout` /
//! `rollback_payout`) so a crash between pay and record can be
//! reconciled on restart instead of re-paying: startup reconciliation
//! (`Ledger::stranded_pending_payouts`) logs every intent still open
//! from a previous run, and the cashu plugin re-delivers minted
//! payloads attached to open intents.

pub mod block_found;
pub mod cashu;
pub mod fedimint;
pub mod ledger;
pub mod lightning;
pub mod registry;
pub mod traits;

#[allow(unused_imports)]
pub use traits::{MinerBalance, PayoutPlugin, PluginContext, PluginError};
