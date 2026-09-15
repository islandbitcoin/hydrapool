//! Registry that fans block events out to plugins and drives payouts.

use crate::payout_plugins::traits::{PluginContext, PayoutPlugin};
use std::sync::Arc;
use tokio::sync::watch;

/// Holds all enabled plugins and forwards block events to each.
pub struct PayoutPluginRegistry {
    plugins: Vec<Arc<dyn PayoutPlugin>>,
}

impl PayoutPluginRegistry {
    pub fn new() -> Self {
        Self { plugins: Vec::new() }
    }

    /// Build a registry from env config. Each plugin enables itself by
    /// presence of its env vars; plugins that fail to configure are
    /// logged and skipped so a broken one can't take the pool down.
    pub fn from_env(ledger_path: std::path::PathBuf) -> Result<Self, String> {
        let ledger = Arc::new(
            crate::payout_plugins::ledger::Ledger::open(ledger_path)
                .map_err(|e| format!("opening payout ledger: {e}"))?,
        );
        let mut registry = Self::new();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::payout_plugins::lightning::LightningPlugin::from_env(ledger.clone())
        })) {
            Ok(Ok(p)) => registry.register(Arc::new(p)),
            Ok(Err(e)) => tracing::info!("Lightning plugin disabled: {e}"),
            Err(_) => tracing::warn!("Lightning plugin init panicked — skipped"),
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::payout_plugins::cashu::CashuPlugin::from_env(ledger.clone())
        })) {
            Ok(Ok(p)) => registry.register(Arc::new(p)),
            Ok(Err(e)) => tracing::info!("Cashu plugin disabled: {e}"),
            Err(_) => tracing::warn!("Cashu plugin init panicked — skipped"),
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::payout_plugins::fedimint::FedimintPlugin::from_env(ledger)
        })) {
            Ok(Ok(p)) => registry.register(Arc::new(p)),
            Ok(Err(e)) => tracing::info!("Fedimint plugin disabled: {e}"),
            Err(_) => tracing::warn!("Fedimint plugin init panicked — skipped"),
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
    /// at/above its threshold. Runs until `shutdown` flips to `true`.
    pub async fn run_payout_loop(&self, interval: std::time::Duration, shutdown: watch::Receiver<bool>) {
        let mut shutdown_rx = shutdown;
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Payout loop shutting down");
                        return;
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
    use std::collections::HashMap;

    struct CountingPlugin {
        blocks: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PayoutPlugin for CountingPlugin {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn on_block(&self, _ctx: &PluginContext) {
            self.blocks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn threshold_sats(&self) -> u64 {
            0
        }
        async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError> {
            Ok(vec![MinerBalance {
                miner_id: "x".into(),
                sats: 1,
            }])
        }
        async fn run(&self, _shutdown: tokio::sync::watch::Receiver<bool>) -> Result<(), PluginError> {
            Ok(())
        }
        fn balances(&self) -> HashMap<String, u64> {
            HashMap::new()
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
            block_hash: "ab".into(),
            block_reward_sats: 312_500_000,
            miner_payouts: vec![],
        });
        registry.on_block(&PluginContext {
            block_height: 901,
            block_hash: "cd".into(),
            block_reward_sats: 312_500_000,
            miner_payouts: vec![],
        });
        assert_eq!(
            plugin.blocks.load(std::sync::atomic::Ordering::SeqCst),
            2
        );
    }
}
