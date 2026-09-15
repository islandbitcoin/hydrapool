//! Core plugin traits and types.

use std::collections::HashMap;

/// A miner's accrued, unpaid balance in satoshis.
///
/// `miner_id` is whatever the miner used to authorize — for coinbase
/// payouts that is a bitcoin address, for plugins it may be a username,
/// npub, lightning address, or Cashu keyset-bound id.
#[derive(Debug, Clone)]
pub struct MinerBalance {
    pub miner_id: String,
    pub sats: u64,
}

/// Context passed to a plugin on each confirmed block.
#[derive(Debug, Clone)]
pub struct PluginContext {
    /// Height of the confirmed block.
    pub block_height: u32,
    /// Block hash (hex).
    pub block_hash: String,
    /// Total block reward (subsidy + fees) in sats.
    pub block_reward_sats: u64,
    /// Per-miner PPLNS share of this block in sats. Sums to the
    /// miner-attributable portion of the reward (after donation/fee cuts).
    pub miner_payouts: Vec<MinerBalance>,
}

/// Error type returned by plugin operations.
#[derive(Debug)]
pub enum PluginError {
    /// Backend (mint, LN node, federation) unreachable or errored.
    Backend(String),
    /// Miner has no registered payout destination — balance is held.
    NoDestination(String),
    /// Payout would violate config (below dust, over limit, ...).
    Rejected(String),
    /// Anything else.
    Other(String),
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginError::Backend(m) => write!(f, "backend error: {m}"),
            PluginError::NoDestination(m) => write!(f, "no destination for miner: {m}"),
            PluginError::Rejected(m) => write!(f, "rejected: {m}"),
            PluginError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for PluginError {}

/// A payout plugin.
///
/// The registry drives plugins: it calls [`PayoutPlugin::on_block`] as
/// confirmed shares arrive and [`PayoutPlugin::payout_due`] on a fixed
/// interval (see [`crate::payout_plugins::registry`]). There is no
/// per-plugin task lifecycle — long-lived work (polling invoice status,
/// federation clients) should be added to the trait only when a real
/// consumer exists.
///
/// The plugin owns its destination mapping: how `miner_id` (the stratum
/// username) resolves to a payout destination (invoice, npub keyset,
/// federation invite code). That mapping is plugin-specific.
#[async_trait::async_trait]
pub trait PayoutPlugin: Send + Sync {
    /// Plugin name, used in logs and metrics.
    fn name(&self) -> &'static str;

    /// Called on every confirmed block with the per-miner PPLNS
    /// distribution. Implementations accrue into their ledger.
    fn on_block(&self, ctx: &PluginContext);

    /// Payout threshold in sats — accruals below this are held.
    fn threshold_sats(&self) -> u64;

    /// Drain all miners whose accrued balance is at or above
    /// [`PayoutPlugin::threshold_sats`], paying out via the plugin's
    /// backend. Returns the miners successfully paid.
    ///
    /// Implementations must follow the ledger's pending-intent protocol:
    /// `begin_payout` before dispatching any payment, `settle_payout`
    /// after confirmation, `rollback_payout` when the payment did not
    /// happen. This keeps pay-then-crash from double-paying or losing
    /// credit.
    async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError>;

    /// Snapshot of accrued balances, for the stats/API surface.
    fn balances(&self) -> HashMap<String, u64>;
}
