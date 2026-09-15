//! Core plugin traits and types.

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

/// Context passed to a plugin on each pool-found block.
#[derive(Debug, Clone)]
pub struct PluginContext {
    /// Height of the pool-found block (from the mined template).
    pub block_height: u32,
    /// Per-miner PPLNS share of this block in sats. Sums to the
    /// miner-attributable reward (coinbase value after the donation
    /// and fee cuts) when the window is non-empty.
    pub miner_payouts: Vec<MinerBalance>,
}

/// Error type returned by plugin operations.
#[derive(Debug)]
pub enum PluginError {
    /// Backend (mint, LN node, federation) unreachable or errored.
    Backend(String),
    /// The request outcome is UNKNOWN (timed out, connection dropped
    /// mid-response, accepted-but-no-operation-id): the payment may
    /// still have gone through. Callers must leave the pending intent
    /// open for reconciliation instead of rolling back — a rollback
    /// here risks paying the miner twice.
    Ambiguous(String),
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
            PluginError::Ambiguous(m) => write!(f, "ambiguous outcome: {m}"),
            PluginError::NoDestination(m) => write!(f, "no destination for miner: {m}"),
            PluginError::Rejected(m) => write!(f, "rejected: {m}"),
            PluginError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for PluginError {}

/// Map a reqwest transport error to a plugin error, treating timeouts
/// as [`PluginError::Ambiguous`]: the request may have reached the
/// backend, so the pending intent must stay open (rolling back here
/// risks double-paying). Use for the payment-dispatch call itself and
/// its response reads, never for pre-dispatch requests.
pub fn ambiguous_on_timeout(label: &'static str) -> impl Fn(reqwest::Error) -> PluginError {
    move |e: reqwest::Error| {
        if e.is_timeout() {
            PluginError::Ambiguous(format!("{label}: {e}"))
        } else {
            PluginError::Backend(format!("{label}: {e}"))
        }
    }
}

/// A payout plugin.
///
/// The registry drives plugins: it calls [`PayoutPlugin::on_block`] on
/// every pool-found block and [`PayoutPlugin::payout_due`] on a fixed
/// interval (see [`crate::payout_plugins::registry`]). There is no
/// per-plugin task lifecycle — long-lived work (polling invoice status,
/// federation clients) should be added to the trait only when a real
/// consumer exists.
///
/// The plugin owns its destination mapping: how `miner_id` (the stratum
/// username) resolves to a payout destination (invoice, npub keyset,
/// federation invite code). That mapping is plugin-specific.
///
/// Error semantics every implementation must follow (the registry's
/// crash-safety depends on this uniformity):
/// - **Timeout / unknown outcome** → [`PluginError::Ambiguous`]: the
///   pending intent stays OPEN; a later pass reconciles instead of
///   re-paying.
/// - **Definite pre-dispatch failure** (backend unreachable and
///   provably before the payment, destination missing, request
///   rejected) → any other variant: the caller rolls the pending
///   intent back and the next pass retries the full flow.
#[async_trait::async_trait]
pub trait PayoutPlugin: Send + Sync {
    /// Plugin name, used in logs and metrics.
    fn name(&self) -> &'static str;

    /// Called on every pool-found block with the per-miner PPLNS
    /// distribution (already net of donation/fee cuts). Implementations
    /// accrue into their ledger.
    fn on_block(&self, ctx: &PluginContext);

    /// Drain all miners whose accrued balance is at or above the
    /// plugin's configured threshold, paying out via the plugin's
    /// backend. Returns the miners successfully paid.
    ///
    /// Implementations must follow the ledger's pending-intent protocol:
    /// `begin_payout` before dispatching any payment, `settle_payout`
    /// after confirmation, `rollback_payout` only when the payment
    /// definitely did not happen (see the error-semantics note on this
    /// trait). This keeps pay-then-crash from double-paying or losing
    /// credit.
    async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError>;
}
