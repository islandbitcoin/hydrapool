//! Registry that fans block events out to plugins and drives payouts.

use crate::payout_plugins::traits::{PayoutPlugin, PluginContext};
use std::sync::Arc;
use tokio::sync::watch;

/// Holds all enabled plugins and forwards block events to each.
pub struct PayoutPluginRegistry {
    plugins: Vec<Arc<dyn PayoutPlugin>>,
}

impl PayoutPluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }

    /// Build a registry from env config. Each plugin enables itself by
    /// presence of its env vars; plugins that fail to configure are
    /// logged and skipped so a broken one can't take the pool down.
    ///
    /// Startup reconciliation happens here, right after the ledger is
    /// opened: pending intents left open by a crash between
    /// `begin_payout` and settle/rollback are surfaced via
    /// [`Ledger::stranded_pending_payouts`] (an error log per intent)
    /// so the operator sees stranded payouts instead of the miner's
    /// debited credit silently vanishing.
    pub fn from_env(ledger_path: std::path::PathBuf) -> Result<Self, String> {
        let ledger = Arc::new(
            crate::payout_plugins::ledger::Ledger::open(ledger_path)
                .map_err(|e| format!("opening payout ledger: {e}"))?,
        );
        let stranded = ledger.stranded_pending_payouts();
        if !stranded.is_empty() {
            tracing::error!(
                count = stranded.len(),
                "payout ledger has {} open pending payout(s) from a previous run — see the per-intent errors above",
                stranded.len()
            );
        }
        let mut registry = Self::new();
        match crate::payout_plugins::lightning::LightningPlugin::from_env(ledger.clone()) {
            Ok(p) => registry.register(Arc::new(p)),
            Err(e) => tracing::info!("Lightning plugin disabled: {e}"),
        }
        match crate::payout_plugins::cashu::CashuPlugin::from_env(ledger.clone()) {
            Ok(p) => registry.register(Arc::new(p)),
            Err(e) => tracing::info!("Cashu plugin disabled: {e}"),
        }
        match crate::payout_plugins::fedimint::FedimintPlugin::from_env(ledger) {
            Ok(p) => registry.register(Arc::new(p)),
            Err(e) => tracing::info!("Fedimint plugin disabled: {e}"),
        }
        Ok(registry)
    }

    pub fn register(&mut self, plugin: Arc<dyn PayoutPlugin>) {
        tracing::info!(plugin = plugin.name(), "Registered payout plugin");
        self.plugins.push(plugin);
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    /// Notify every plugin of a confirmed block's distribution.
    pub fn on_block(&self, ctx: &PluginContext) {
        for plugin in &self.plugins {
            plugin.on_block(ctx);
        }
    }

    /// Spawn the payout loop: on `interval`, each plugin drains balances
    /// at/above its threshold. Runs until `shutdown` flips to `true` or
    /// the shutdown sender is dropped (sender dropped = the pool is
    /// shutting down; treat it as a stop signal, not a keep-alive).
    pub async fn run_payout_loop(
        &self,
        interval: std::time::Duration,
        shutdown: watch::Receiver<bool>,
    ) {
        let mut shutdown_rx = shutdown;
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                changed = shutdown_rx.changed() => {
                    match changed {
                        // Sender dropped: the process is going away.
                        Err(_) => {
                            tracing::info!("Payout loop shutting down (shutdown sender dropped)");
                            return;
                        }
                        Ok(()) if *shutdown_rx.borrow() => {
                            tracing::info!("Payout loop shutting down");
                            return;
                        }
                        Ok(()) => {}
                    }
                }
            }
            for plugin in &self.plugins {
                match plugin.payout_due().await {
                    Ok(paid) if !paid.is_empty() => {
                        for payout in paid {
                            tracing::info!(
                                plugin = plugin.name(),
                                miner = %payout.miner_id,
                                sats = payout.sats,
                                "Plugin payout complete"
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(plugin = plugin.name(), "Payout pass failed: {e}");
                    }
                }
            }
        }
    }
}

impl Default for PayoutPluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payout_plugins::traits::{MinerBalance, PluginError};

    struct CountingPlugin {
        blocks: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PayoutPlugin for CountingPlugin {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn on_block(&self, _ctx: &PluginContext) {
            self.blocks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError> {
            Ok(vec![MinerBalance {
                miner_id: "x".into(),
                sats: 1,
            }])
        }
    }

    #[tokio::test]
    async fn fans_out_block_events() {
        let plugin = Arc::new(CountingPlugin {
            blocks: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut registry = PayoutPluginRegistry::new();
        registry.register(plugin.clone());
        registry.on_block(&PluginContext {
            block_height: 900,
            miner_payouts: vec![],
        });
        registry.on_block(&PluginContext {
            block_height: 901,
            miner_payouts: vec![],
        });
        assert_eq!(plugin.blocks.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn payout_loop_exits_on_shutdown_true() {
        let registry = std::sync::Arc::new(PayoutPluginRegistry::new());
        let (tx, rx) = watch::channel(false);
        let loop_task = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .run_payout_loop(std::time::Duration::from_millis(10), rx)
                    .await
            })
        };
        tx.send(true).unwrap();
        // Must return promptly once shutdown flips true.
        tokio::time::timeout(std::time::Duration::from_secs(2), loop_task)
            .await
            .expect("loop must exit on shutdown=true")
            .unwrap();
    }

    #[tokio::test]
    async fn payout_loop_exits_when_sender_dropped() {
        let registry = std::sync::Arc::new(PayoutPluginRegistry::new());
        let (tx, rx) = watch::channel(false);
        let loop_task = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .run_payout_loop(std::time::Duration::from_millis(10), rx)
                    .await
            })
        };
        drop(tx);
        // Sender dropped must be treated as shutdown, not keep-alive.
        tokio::time::timeout(std::time::Duration::from_secs(2), loop_task)
            .await
            .expect("loop must exit when the shutdown sender is dropped")
            .unwrap();
    }
}
