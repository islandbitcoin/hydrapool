//! Payout plugin system for HydraPool.
//!
//! HydraPool's native payout path is on-chain coinbase outputs (max 100
//! payees). Plugins extend payouts to Lightning (BOLT12/BOLT11 invoices)
//! and eCash (Cashu ecash, Fedimint) by intercepting the block-found
//! event and paying out a miner's accrued balance out-of-band.
//!
//! Flow:
//! 1. [`PayoutPluginRegistry`] is notified on every confirmed block.
//! 2. Each plugin holds a [`PluginLedger`] of accrued per-miner balances
//!    (keyed by the miner's identifier — username, npub, or lightning
//!    address as given in the stratum authorize line).
//! 3. When a miner's balance crosses the plugin's threshold, the plugin
//!    pays out via its backend (LND/CLN gRPC, mint REST, federation) and
//!    records the payout in the ledger.
//!
//! Failures are retried with backoff; a payout that cannot complete
//! leaves the balance untouched (credit) or marks it pending (debit),
//! never lost.

pub mod cashu;
pub mod fedimint;
pub mod ledger;
pub mod lightning;
pub mod registry;
pub mod traits;

pub use traits::{MinerBalance, PayoutPlugin, PluginContext, PluginError};
