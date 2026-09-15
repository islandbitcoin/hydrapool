//! Payout plugin system for HydraPool.
//!
//! HydraPool's native payout path is on-chain coinbase outputs (max 100
//! payees). Plugins extend payouts to Lightning (BOLT12/BOLT11 invoices)
//! and eCash (Cashu ecash, Fedimint) by intercepting the block-found
//! event and paying out a miner's accrued balance out-of-band.
//!
//! Flow:
//! 1. [`PayoutPluginRegistry`] is notified on every confirmed block
//!    (see `src/main.rs`, which forwards confirmed share events into
//!    `registry.on_block`).
//! 2. All plugins share one [`crate::payout_plugins::ledger::Ledger`] of
//!    accrued per-miner balances (keyed by the miner's identifier —
//!    username, npub, or lightning address as given in the stratum
//!    authorize line), namespaced per plugin.
//! 3. On a fixed 60s tick, the registry calls each plugin's
//!    `payout_due`; the plugin pays out via its backend (LND REST,
//!    mint REST, federation gateway) and records the payout in the
//!    ledger.
//!
//! Failures are retried on the next fixed-interval tick (there is no
//! backoff); a payout that cannot complete leaves the balance untouched,
//! never lost. A payment dispatched but not settled is bracketed by
//! pending-intent ledger entries (`begin_payout` / `settle_payout` /
//! `rollback_payout`) so a crash between pay and record can be
//! reconciled on restart instead of re-paying.

pub mod cashu;
pub mod fedimint;
pub mod ledger;
pub mod lightning;
pub mod registry;
pub mod traits;

#[allow(unused_imports)]
pub use traits::{MinerBalance, PayoutPlugin, PluginContext, PluginError};
