//! Registry that fans block events out to plugins and drives payouts.
//!
//! Payout scheduling is parallel per rail. The fixed-interval tick does
//! NOT run payouts inline: it only signals each rail's dedicated drain
//! task (one tokio task per enabled plugin, woken via a per-plugin
//! [`tokio::sync::Notify`]), so a slow or hung rail never delays
//! another rail's tick. Each drain task exclusively owns its rail —
//! two concurrent drains of the SAME rail never happen by construction
//! — while drains across DIFFERENT rails do run concurrently, which is
//! safe because pending intents are per (plugin, miner) in the ledger
//! and every rail only touches its own ledger namespace.
//!
//! Each drain attempt is bounded by a per-rail timeout (default 120s,
//! override per rail with `<prefix>_DRAIN_TIMEOUT_SECS`, e.g.
//! `HYDRA_LN_DRAIN_TIMEOUT_SECS` / `HYDRA_CASHU_DRAIN_TIMEOUT_SECS` /
//! `HYDRA_FEDIMINT_DRAIN_TIMEOUT_SECS`). A timed-out attempt simply
//! cancels the drain future; any payment it already dispatched is
//! bracketed by a pending intent in the ledger and is reconciled on
//! restart (or re-delivered by the cashu plugin), never re-paid.
//!
//! A failed attempt is retried with exponential backoff starting at
//! [`PayoutPluginRegistry::retry_backoff_initial`] and capped at
//! [`PayoutPluginRegistry::retry_backoff_max`]; a success resets the
//! backoff and the failure counter.
//!
//! Circuit breaker: after [`DEGRADED_AFTER_FAILURES`] consecutive
//! failed attempts a rail is marked `degraded` — tick-driven drains
//! stop (accrual via `on_block` is unaffected), the rail is slow-probed
//! once per [`PayoutPluginRegistry::retry_backoff_max`] so it can
//! recover without operator intervention, and an error is logged every
//! [`DEGRADED_LOG_EVERY_TICKS`] ticks with the failure count and last
//! error. Per-rail state is exposed for metrics via
//! [`PayoutPluginRegistry::rail_status`].

use crate::payout_plugins::traits::{MinerBalance, PayoutPlugin, PluginContext};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, watch};

/// Consecutive failed drain attempts before a rail is marked degraded.
pub const DEGRADED_AFTER_FAILURES: u32 = 5;

/// While degraded, log the standing error every N ticks.
const DEGRADED_LOG_EVERY_TICKS: u32 = 30;

/// Default per-drain timeout when the rail's
/// `<prefix>_DRAIN_TIMEOUT_SECS` env var is unset.
pub const DRAIN_TIMEOUT_DEFAULT: std::time::Duration = std::time::Duration::from_secs(120);

/// Health state of one rail, tracked by its drain task.
#[derive(Default)]
struct RailHealth {
    consecutive_failures: u32,
    degraded: bool,
    /// Ticks observed since the rail was marked degraded.
    degraded_ticks: u32,
    last_error: Option<String>,
}

/// Point-in-time health of one rail, for metrics/monitoring.
// Consumed by the metrics surface in a follow-up; kept public API now
// so the shape is stable before dashboards read it.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RailStatus {
    pub name: &'static str,
    /// true = healthy (drains scheduled on ticks); false = degraded
    /// (tick drains suspended, slow-probing at `retry_backoff_max`).
    pub healthy: bool,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

/// Holds all enabled plugins, forwards block events to each, and runs
/// one drain task per plugin so payouts proceed in parallel per rail.
pub struct PayoutPluginRegistry {
    plugins: Vec<Arc<dyn PayoutPlugin>>,
    health: Vec<Arc<Mutex<RailHealth>>>,
    signals: Vec<Arc<Notify>>,
    /// Per-drain timeout used when the rail's
    /// `<prefix>_DRAIN_TIMEOUT_SECS` env var is unset.
    pub drain_timeout: std::time::Duration,
    /// First retry delay after a failed drain attempt; doubles on each
    /// consecutive failure up to `retry_backoff_max`.
    pub retry_backoff_initial: std::time::Duration,
    /// Retry backoff cap; also the degraded-rail slow-probe interval.
    pub retry_backoff_max: std::time::Duration,
}

impl PayoutPluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
            health: Vec::new(),
            signals: Vec::new(),
            drain_timeout: DRAIN_TIMEOUT_DEFAULT,
            retry_backoff_initial: std::time::Duration::from_secs(1),
            retry_backoff_max: std::time::Duration::from_secs(600),
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
        // With every rail feature disabled nothing registers, so the
        // `mut` is only needed when at least one rail is compiled in.
        #[cfg_attr(
            not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
            allow(unused_mut)
        )]
        let mut registry = Self::new();
        #[cfg(feature = "ln")]
        match crate::payout_plugins::lightning::LightningPlugin::from_env(ledger.clone()) {
            Ok(p) => registry.register(Arc::new(p)),
            Err(e) => tracing::info!("Lightning plugin disabled: {e}"),
        }
        #[cfg(feature = "cashu")]
        match crate::payout_plugins::cashu::CashuPlugin::from_env(ledger.clone()) {
            Ok(p) => registry.register(Arc::new(p)),
            Err(e) => tracing::info!("Cashu plugin disabled: {e}"),
        }
        #[cfg(feature = "fedimint")]
        match crate::payout_plugins::fedimint::FedimintPlugin::from_env(ledger) {
            Ok(p) => registry.register(Arc::new(p)),
            Err(e) => tracing::info!("Fedimint plugin disabled: {e}"),
        }
        // With every rail feature disabled the ledger Arc above would
        // be unused (nothing registers); consume it to keep compiling.
        #[cfg(not(any(feature = "ln", feature = "cashu", feature = "fedimint")))]
        drop(ledger);
        Ok(registry)
    }

    // Called by rail registrations in from_env; the all-rails-disabled
    // build (which registers nothing) would otherwise flag it dead.
    #[cfg_attr(
        not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
        allow(dead_code)
    )]
    pub fn register(&mut self, plugin: Arc<dyn PayoutPlugin>) {
        tracing::info!(plugin = plugin.name(), "Registered payout plugin");
        self.plugins.push(plugin);
        self.health
            .push(Arc::new(Mutex::new(RailHealth::default())));
        self.signals.push(Arc::new(Notify::new()));
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    /// Current per-rail health (name, healthy/degraded, failure count,
    /// last error) for metrics/monitoring surfaces.
    // Consumed by the metrics surface in a follow-up; kept public API
    // now so the shape is stable before dashboards read it.
    #[allow(dead_code)]
    pub fn rail_status(&self) -> Vec<RailStatus> {
        self.plugins
            .iter()
            .zip(&self.health)
            .map(|(plugin, health)| {
                let h = health.lock().unwrap();
                RailStatus {
                    name: plugin.name(),
                    healthy: !h.degraded,
                    consecutive_failures: h.consecutive_failures,
                    last_error: h.last_error.clone(),
                }
            })
            .collect()
    }

    /// Per-rail drain timeout: the rail's `<prefix>_DRAIN_TIMEOUT_SECS`
    /// env var if set and parseable, else the registry default.
    fn effective_drain_timeout(&self, plugin: &dyn PayoutPlugin) -> std::time::Duration {
        let var = format!("{}_DRAIN_TIMEOUT_SECS", plugin.env_prefix());
        match std::env::var(&var).ok().and_then(|v| v.parse::<u64>().ok()) {
            Some(secs) => {
                tracing::info!(
                    plugin = plugin.name(),
                    var = %var,
                    secs,
                    "Using env-configured drain timeout"
                );
                std::time::Duration::from_secs(secs)
            }
            None => self.drain_timeout,
        }
    }

    /// Notify every plugin of a confirmed block's distribution.
    pub fn on_block(&self, ctx: &PluginContext) {
        for plugin in &self.plugins {
            plugin.on_block(ctx);
        }
    }

    /// Spawn the payout loop: on `interval`, signal every rail's drain
    /// task. Each drain task runs its plugin's `payout_due` with a
    /// per-rail timeout and retry/backoff, entirely off the tick path —
    /// see the module docs for the parallelism and circuit-breaker
    /// semantics. Runs until `shutdown` flips to `true` or the shutdown
    /// sender is dropped (sender dropped = the pool is shutting down;
    /// treat it as a stop signal, not a keep-alive).
    pub async fn run_payout_loop(
        &self,
        interval: std::time::Duration,
        shutdown: watch::Receiver<bool>,
    ) {
        // One drain task per rail, each woken by its own Notify. A slow
        // rail backs up inside its own task only; the tick loop below
        // never awaits a payout.
        for (plugin, health) in self.plugins.iter().zip(&self.health) {
            let plugin = plugin.clone();
            let health = health.clone();
            let signal = self.signals[self
                .plugins
                .iter()
                .position(|p| Arc::ptr_eq(p, &plugin))
                .unwrap_or(0)]
            .clone();
            let drain_timeout = self.effective_drain_timeout(plugin.as_ref());
            let shutdown_rx = shutdown.clone();
            let backoff_initial = self.retry_backoff_initial;
            let backoff_max = self.retry_backoff_max;
            tokio::spawn(drain_task(
                plugin,
                signal,
                health,
                drain_timeout,
                backoff_initial,
                backoff_max,
                shutdown_rx,
            ));
        }
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
            for signal in &self.signals {
                // notify_one stores a permit while the drain task is
                // busy, so ticks that land mid-drain are not lost — the
                // task drains again as soon as it frees up.
                signal.notify_one();
            }
        }
    }
}

/// One timeout-bounded drain pass. Cancelling on timeout is safe: any
/// payment already dispatched left a pending intent in the ledger for
/// reconciliation (see [`crate::payout_plugins::traits::PayoutPlugin`]).
async fn drain_attempt(
    plugin: &dyn PayoutPlugin,
    drain_timeout: std::time::Duration,
) -> Result<Vec<MinerBalance>, String> {
    match tokio::time::timeout(drain_timeout, plugin.payout_due()).await {
        Ok(Ok(paid)) => Ok(paid),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("drain timed out after {drain_timeout:?}")),
    }
}

fn record_success(health: &Mutex<RailHealth>) {
    let mut h = health.lock().unwrap();
    h.consecutive_failures = 0;
    h.degraded = false;
    h.degraded_ticks = 0;
}

/// Record a failure; returns true when this failure flipped the rail
/// to degraded (logged exactly once, at the transition).
fn record_failure(health: &Mutex<RailHealth>, msg: String, name: &str) -> bool {
    let mut h = health.lock().unwrap();
    h.last_error = Some(msg.clone());
    h.consecutive_failures += 1;
    if !h.degraded && h.consecutive_failures >= DEGRADED_AFTER_FAILURES {
        h.degraded = true;
        h.degraded_ticks = 0;
        tracing::error!(
            plugin = name,
            consecutive_failures = h.consecutive_failures,
            last_error = %msg,
            "payout rail marked degraded after {} consecutive failures — tick drains suspended (accrual continues), probing every {:?}",
            DEGRADED_AFTER_FAILURES,
            std::time::Duration::from_secs(600)
        );
        true
    } else {
        false
    }
}

/// Per-rail drain task: exclusively owns one plugin. Waits for tick
/// signals, drains with timeout + bounded exponential backoff, and
/// while degraded only slow-probes at `backoff_max` until a drain
/// succeeds again.
#[allow(clippy::too_many_arguments)]
async fn drain_task(
    plugin: Arc<dyn PayoutPlugin>,
    _signal: Arc<Notify>,
    health: Arc<Mutex<RailHealth>>,
    drain_timeout: std::time::Duration,
    backoff_initial: std::time::Duration,
    backoff_max: std::time::Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let name = plugin.name();
    // The signal Notify is owned by the registry; drain_task receives a
    // clone so the task can wait on it independently.
    let signal = _signal;
    // Degraded slow-probe timer: one attempt per `backoff_max` so a
    // degraded rail can recover without tick-driven scheduling.
    let mut probe = tokio::time::interval(backoff_max);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut degraded_phase = false;
    loop {
        if degraded_phase {
            tokio::select! {
                _ = signal.notified() => {
                    // Ticks only log while degraded — they never drain.
                    let mut h = health.lock().unwrap();
                    h.degraded_ticks += 1;
                    if h.degraded_ticks.is_multiple_of(DEGRADED_LOG_EVERY_TICKS) {
                        tracing::error!(
                            plugin = name,
                            consecutive_failures = h.consecutive_failures,
                            last_error = h.last_error.as_deref().unwrap_or(""),
                            "payout rail still degraded — tick drains suspended, probing every {:?}",
                            backoff_max
                        );
                    }
                }
                _ = probe.tick() => {
                    match drain_attempt(plugin.as_ref(), drain_timeout).await {
                        Ok(paid) => {
                            record_success(&health);
                            for payout in &paid {
                                tracing::info!(
                                    plugin = name,
                                    miner = %payout.miner_id,
                                    sats = payout.sats,
                                    "Plugin payout complete"
                                );
                            }
                            tracing::info!(
                                plugin = name,
                                "payout rail recovered — resuming tick-driven drains"
                            );
                            degraded_phase = false;
                        }
                        Err(msg) => {
                            record_failure(&health, msg, name);
                        }
                    }
                }
                changed = shutdown_rx.changed() => {
                    match changed {
                        Err(_) => return,
                        Ok(()) if *shutdown_rx.borrow() => return,
                        Ok(()) => {}
                    }
                }
            }
            continue;
        }
        tokio::select! {
            _ = signal.notified() => {}
            changed = shutdown_rx.changed() => {
                match changed {
                    Err(_) => return,
                    Ok(()) if *shutdown_rx.borrow() => return,
                    Ok(()) => continue,
                }
            }
        }
        // Healthy drain pass: retry failures with exponential backoff
        // (capped) until success resets, or the breaker trips.
        let mut backoff = backoff_initial;
        loop {
            match drain_attempt(plugin.as_ref(), drain_timeout).await {
                Ok(paid) => {
                    record_success(&health);
                    for payout in &paid {
                        tracing::info!(
                            plugin = name,
                            miner = %payout.miner_id,
                            sats = payout.sats,
                            "Plugin payout complete"
                        );
                    }
                    break;
                }
                Err(msg) => {
                    if record_failure(&health, msg, name) {
                        degraded_phase = true;
                        break;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        changed = shutdown_rx.changed() => {
                            match changed {
                                Err(_) => return,
                                Ok(()) if *shutdown_rx.borrow() => return,
                                Ok(()) => {}
                            }
                        }
                    }
                    backoff = backoff.saturating_mul(2).min(backoff_max);
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
    use std::sync::atomic::{AtomicBool, Ordering};

    struct CountingPlugin {
        blocks: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PayoutPlugin for CountingPlugin {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn env_prefix(&self) -> &'static str {
            "HYDRA_TEST_COUNTING"
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

    /// Drain counter with a configurable pre-drain sleep, for the
    /// parallelism and timeout tests.
    struct SleepyPlugin {
        delay: std::time::Duration,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PayoutPlugin for SleepyPlugin {
        fn name(&self) -> &'static str {
            "sleepy"
        }
        fn env_prefix(&self) -> &'static str {
            "HYDRA_TEST_SLEEPY"
        }
        fn on_block(&self, _ctx: &PluginContext) {}
        async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(vec![])
        }
    }

    /// Flips from always-fail to always-succeed on demand, for the
    /// circuit-breaker tests.
    struct RecoverablePlugin {
        healthy: AtomicBool,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl PayoutPlugin for RecoverablePlugin {
        fn name(&self) -> &'static str {
            "recoverable"
        }
        fn env_prefix(&self) -> &'static str {
            "HYDRA_TEST_RECOVERABLE"
        }
        fn on_block(&self, _ctx: &PluginContext) {}
        async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.healthy.load(Ordering::SeqCst) {
                Ok(vec![])
            } else {
                Err(PluginError::Backend("mock backend down".into()))
            }
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

    /// The point of parallel drains: a rail stuck in a slow payout_due
    /// must not throttle the other rails' ticks.
    #[tokio::test]
    async fn slow_rail_does_not_delay_other_rails() {
        let slow = Arc::new(SleepyPlugin {
            delay: std::time::Duration::from_millis(300),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let fast = Arc::new(SleepyPlugin {
            delay: std::time::Duration::ZERO,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut registry = PayoutPluginRegistry::new();
        registry.retry_backoff_initial = std::time::Duration::from_millis(5);
        registry.register(slow.clone());
        registry.register(fast.clone());
        let registry = Arc::new(registry);
        let (_tx, rx) = watch::channel(false);
        let loop_task = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .run_payout_loop(std::time::Duration::from_millis(10), rx)
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        _tx.send(true).unwrap();
        let _ = loop_task.await;
        // Run sequentially, a 300ms drain per tick would yield ~2 fast
        // passes in 500ms; parallel drains give the fast rail many.
        let fast_calls = fast.calls.load(Ordering::SeqCst);
        assert!(
            fast_calls >= 5,
            "fast rail must drain on many ticks while the slow rail is stuck (got {fast_calls})"
        );
        assert!(slow.calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn circuit_breaker_degrades_then_recovers() {
        let plugin = Arc::new(RecoverablePlugin {
            healthy: AtomicBool::new(false),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut registry = PayoutPluginRegistry::new();
        registry.retry_backoff_initial = std::time::Duration::from_millis(2);
        registry.retry_backoff_max = std::time::Duration::from_millis(20);
        registry.register(plugin.clone());
        let registry = Arc::new(registry);
        let (tx, rx) = watch::channel(false);
        let loop_task = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .run_payout_loop(std::time::Duration::from_millis(5), rx)
                    .await
            })
        };
        // Degrade after DEGRADED_AFTER_FAILURES consecutive failures.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let status = &registry.rail_status()[0];
                if !status.healthy {
                    assert_eq!(status.name, "recoverable");
                    assert!(status.consecutive_failures >= DEGRADED_AFTER_FAILURES);
                    assert!(
                        status
                            .last_error
                            .as_deref()
                            .unwrap_or_default()
                            .contains("mock backend down")
                    );
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("rail must degrade after consecutive failures");
        // While degraded, ticks must not schedule drains: only the slow
        // probe (retry_backoff_max) attempts payouts. Freeze the backend
        // and confirm calls only creep up at probe pace, not tick pace.
        let calls_degraded = plugin.calls.load(Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        let probe_calls = plugin.calls.load(Ordering::SeqCst) - calls_degraded;
        assert!(
            probe_calls <= 6,
            "degraded rail must probe slowly, not per tick (grew by {probe_calls})"
        );
        // Heal the backend; the next probe must recover the rail.
        plugin.healthy.store(true, Ordering::SeqCst);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let status = &registry.rail_status()[0];
                if status.healthy {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("rail must recover via slow probe once the backend heals");
        tx.send(true).unwrap();
        let _ = loop_task;
    }

    #[tokio::test]
    async fn drain_timeout_fails_the_pass_and_is_recorded() {
        let plugin = Arc::new(SleepyPlugin {
            delay: std::time::Duration::from_millis(200),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut registry = PayoutPluginRegistry::new();
        registry.drain_timeout = std::time::Duration::from_millis(20);
        registry.retry_backoff_initial = std::time::Duration::from_millis(2);
        registry.register(plugin.clone());
        let registry = Arc::new(registry);
        let (tx, rx) = watch::channel(false);
        let loop_task = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .run_payout_loop(std::time::Duration::from_millis(5), rx)
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let status = &registry.rail_status()[0];
                if !status.healthy
                    && status
                        .last_error
                        .as_deref()
                        .unwrap_or_default()
                        .contains("timed out")
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("over-threshold drain must time out and eventually degrade the rail");
        tx.send(true).unwrap();
        let _ = loop_task;
    }
}
