// Copyright (C) 2024-2026 Hydrapool Developers (see AUTHORS)
//
// This file is part of Hydrapool.
//
// Hydrapool is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// Hydrapool is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with
// Hydrapool. If not, see <https://www.gnu.org/licenses/>.

use clap::Parser;
use p2poolv2_api::start_api_server;
use p2poolv2_lib::accounting::payout::sharechain_pplns::Payout;
use p2poolv2_lib::accounting::stats::metrics;
use p2poolv2_lib::config::Config;
use p2poolv2_lib::logging::setup_logging;
use p2poolv2_lib::node::actor::NodeHandle;
use p2poolv2_lib::pool_difficulty::PoolDifficulty;
use p2poolv2_lib::shares::chain::chain_store_handle::ChainStoreHandle;
use p2poolv2_lib::shares::share_block::ShareBlock;
use p2poolv2_lib::store::Store;
use p2poolv2_lib::store::writer::{StoreHandle, StoreWriter, write_channel};
use p2poolv2_lib::stratum::client_connections::start_connections_handler;
use p2poolv2_lib::stratum::emission::Emission;
use p2poolv2_lib::stratum::server::StratumServerBuilder;
use p2poolv2_lib::stratum::work::gbt::start_gbt;
use p2poolv2_lib::stratum::work::notify::start_notify;
use p2poolv2_lib::stratum::work::tracker::start_tracker_actor;
use p2poolv2_lib::stratum::zmq_listener::{ZmqListener, ZmqListenerTrait};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, trace, warn};

use crate::signal::{ShutdownReason, setup_signal_handler};

mod background_tasks;
mod payout_plugins;
mod signal;

/// Interval in seconds to poll for new block templates since the last zmq event signal
const GBT_POLL_INTERVAL: u64 = 10; // seconds

/// Maximum number of pending shares from all clients connected to stratum server
const STRATUM_SHARES_BUFFER_SIZE: usize = 1000;

/// 100% donation in bips, skip address validation
const FULL_DONATION_BIPS: u16 = 10_000;

/// Notify channel enqueues requests to send notify updates to new
/// clients. If we have more than the notify channel capacity of
/// pending notifications in the queue, senders are blocked unless
/// space is available. We want to avoid this blocking for up to 1000
/// notifications from new clients.
const NOTIFY_CHANNEL_CAPACITY: usize = 1000;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env("HYDRAPOOL_CONFIG"))]
    config: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    info!("Starting Hydrapool...");
    // Parse command line arguments
    let args = Args::parse();

    // Load configuration
    let config = Config::load(&args.config);
    if config.is_err() {
        let err = config.unwrap_err();
        error!("Failed to load config: {err}");
        return ExitCode::FAILURE;
    }
    let config = config.unwrap();
    // Configure logging based on config
    // hold guards to keep non-blocking writers alive
    let _guards = match setup_logging(&config.logging) {
        Ok(guards) => {
            info!("Logging set up successfully");
            guards
        }
        Err(e) => {
            error!("Failed to set up logging: {e}");
            return ExitCode::FAILURE;
        }
    };

    info!("Running on {} network", &config.stratum.network);

    let exit_sender = tokio::sync::watch::Sender::new(ShutdownReason::None);

    let sig_handle = setup_signal_handler(exit_sender.clone());

    let genesis = ShareBlock::build_genesis_for_network(config.stratum.network).unwrap();
    let store = Arc::new(Store::new(config.store.path.clone(), false).unwrap());

    // Create StoreWriter for serialized database writes (runs on dedicated blocking thread)
    let (write_tx, write_rx) = write_channel();
    let store_writer = StoreWriter::new(store.clone(), write_rx);
    // Use spawn_blocking to spawn a dedicated thread to write to
    // rocksdb.  Rocks does support concurrent writes, however, the
    // throughput doesn't scale linearly with threads. We still with a
    // single thread, if we need to start another thread as scale
    // increases, we can deal with that then.
    let exit_sender_store = exit_sender.clone();
    let exit_receiver_store = exit_sender.subscribe();
    tokio::task::spawn_blocking(move || {
        store_writer.run();
        if *exit_receiver_store.borrow() == ShutdownReason::None {
            tracing::error!("Store writer stopped unexpectedly");
            let _ = exit_sender_store.send(ShutdownReason::Error);
        }
    });

    // Create StoreHandle and ChainStoreHandle for new components
    let store_handle = StoreHandle::new(store.clone(), write_tx);
    let chain_store_handle = ChainStoreHandle::new(store_handle, config.stratum.network);

    // Initialise chain with genesis (async)
    if let Err(e) = chain_store_handle
        .init_or_setup_genesis(genesis.clone())
        .await
    {
        error!("Failed to initialise chain: {e}");
        return ExitCode::FAILURE;
    }

    let Ok(tip) = chain_store_handle.get_chain_tip() else {
        error!("No chain tip found. Exiting.");
        return ExitCode::FAILURE;
    };
    let Ok(Some(height)) = chain_store_handle.get_tip_height() else {
        error!("No chain tip found. Exiting.");
        return ExitCode::FAILURE;
    };
    info!("Latest tip {} at height {}", tip, height);

    background_tasks::start_background_tasks(
        store.clone(),
        Duration::from_secs(
            config.store.background_task_frequency_hours * background_tasks::SECONDS_PER_HOUR,
        ),
        Duration::from_secs(config.store.pplns_ttl_days * background_tasks::SECONDS_PER_DAY),
        exit_sender.clone(),
        exit_sender.subscribe(),
    );

    let stratum_config = config.stratum.clone().parse().unwrap();
    let bitcoinrpc_config = config.bitcoinrpc.clone();

    let (stratum_shutdown_tx, stratum_shutdown_rx) = tokio::sync::oneshot::channel();
    let (notify_tx, notify_rx) = tokio::sync::mpsc::channel(NOTIFY_CHANNEL_CAPACITY);
    let tracker_handle = start_tracker_actor();

    let notify_tx_for_gbt = notify_tx.clone();
    let bitcoinrpc_config_cloned = bitcoinrpc_config.clone();
    // Setup ZMQ publisher for block notifications
    let zmq_trigger_rx = match ZmqListener.start(&stratum_config.zmqpubhashblock) {
        Ok(rx) => rx,
        Err(e) => {
            error!("Failed to set up ZMQ publisher: {e}");
            return ExitCode::FAILURE;
        }
    };

    let exit_sender_gbt = exit_sender.clone();
    let exit_receiver_gbt = exit_sender.subscribe();
    tokio::spawn(async move {
        if let Err(e) = start_gbt(
            bitcoinrpc_config_cloned,
            notify_tx_for_gbt,
            GBT_POLL_INTERVAL,
            stratum_config.network,
            zmq_trigger_rx,
        )
        .await
        {
            if *exit_receiver_gbt.borrow() == ShutdownReason::None {
                tracing::error!("Failed to fetch block template. Shutting down. \n {e}");
                let _ = exit_sender_gbt.send(ShutdownReason::Error);
            }
        }
    });

    let connections_handle = start_connections_handler().await;

    // Watch channel for broadcasting prepared templates to all connection handlers
    let (template_tx, template_rx) = tokio::sync::watch::channel(None);

    let chain_store_handle_for_notify = chain_store_handle.clone();

    let pool_difficulty_for_notify =
        PoolDifficulty::build(&chain_store_handle).expect("Failed to build pool difficulty");

    let cloned_stratum_config = stratum_config.clone();
    let payout = Payout::new(cloned_stratum_config.network);
    let shared_pplns_window = payout.shared_pplns_window();
    // Keep a window handle for the payout plugins' per-block accrual.
    let plugin_pplns_window = shared_pplns_window.clone();
    let exit_sender_notify = exit_sender.clone();
    let exit_receiver_notify = exit_sender.subscribe();
    tokio::spawn(async move {
        info!("Starting Stratum notifier...");
        start_notify(
            notify_rx,
            template_tx,
            chain_store_handle_for_notify,
            &cloned_stratum_config,
            Box::new(payout),
            pool_difficulty_for_notify,
        )
        .await;
        if *exit_receiver_notify.borrow() == ShutdownReason::None {
            error!("Notifier stopped unexpectedly");
            let _ = exit_sender_notify.send(ShutdownReason::Error);
        }
    });

    let (emissions_tx, emissions_rx) =
        tokio::sync::mpsc::channel::<Emission>(STRATUM_SHARES_BUFFER_SIZE);

    let metrics_handle = match metrics::start_metrics(config.logging.stats_dir.clone()).await {
        Ok(handle) => handle,
        Err(e) => {
            error!("Failed to start metrics: {e}");
            return ExitCode::FAILURE;
        }
    };
    let metrics_cloned = metrics_handle.clone();
    let metrics_for_shutdown = metrics_handle.clone();
    let stats_dir_for_shutdown = config.logging.stats_dir.clone();
    let chain_store_handle_for_stratum = chain_store_handle.clone();
    let tracker_handle_cloned = tracker_handle.clone();
    let notify_tx_for_node = notify_tx.clone();
    let exit_sender_stratum = exit_sender.clone();
    let exit_receiver_stratum = exit_sender.subscribe();

    tokio::spawn(async move {
        let mut stratum_server = StratumServerBuilder::default()
            .shutdown_rx(stratum_shutdown_rx)
            .connections_handle(connections_handle.clone())
            .emissions_tx(emissions_tx)
            .hostname(stratum_config.hostname)
            .port(stratum_config.port)
            .start_difficulty(stratum_config.start_difficulty)
            .minimum_difficulty(stratum_config.minimum_difficulty)
            .maximum_difficulty(stratum_config.maximum_difficulty)
            .ignore_difficulty(stratum_config.ignore_difficulty)
            .validate_addresses(Some(
                stratum_config.donation.unwrap_or_default() != FULL_DONATION_BIPS,
            ))
            .network(stratum_config.network)
            .version_mask(stratum_config.version_mask)
            .max_connections(stratum_config.max_connections)
            .chain_store_handle(chain_store_handle_for_stratum)
            .wait_for_chain_sync(stratum_config.wait_for_chain_sync)
            .build()
            .await
            .unwrap();
        info!("Starting Stratum server...");
        let result = stratum_server
            .start(
                None,
                notify_tx,
                tracker_handle_cloned,
                bitcoinrpc_config,
                metrics_cloned,
                template_rx,
            )
            .await;
        if result.is_err() && *exit_receiver_stratum.borrow() == ShutdownReason::None {
            error!("Failed to start Stratum server: {}", result.unwrap_err());
            let _ = exit_sender_stratum.send(ShutdownReason::Error);
        }
        info!("Stratum server stopped");
    });

    let (monitoring_event_sender, _monitoring_event_receiver) =
        p2poolv2_lib::monitoring_events::create_monitoring_event_channel();
    // Receiver for the payout plugins, subscribed before the sender is
    // moved into the API server.
    let plugin_event_rx = monitoring_event_sender.subscribe();

    let (node_handle, stopping_rx) = match NodeHandle::new(
        config.clone(),
        chain_store_handle.clone(),
        emissions_rx,
        metrics_handle.clone(),
        monitoring_event_sender.clone(),
        notify_tx_for_node,
        shared_pplns_window,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            error!("Failed to start node: {e}");
            return ExitCode::FAILURE;
        }
    };

    info!("Node started");

    let api_shutdown_tx = match start_api_server(
        config.api.clone(),
        chain_store_handle.clone(),
        metrics_handle.clone(),
        tracker_handle,
        node_handle.clone(),
        monitoring_event_sender,
        stratum_config.network,
        stratum_config.pool_signature,
    )
    .await
    {
        Ok((shutdown_tx, _port)) => shutdown_tx,
        Err(e) => {
            error!("Error starting API server: {e}");
            return ExitCode::FAILURE;
        }
    };
    info!(
        "API server started on host {} port {}",
        config.api.hostname, config.api.port
    );

    // Payout plugins: Lightning / Cashu / Fedimint out-of-band payouts.
    // Enabled independently by their env config; all disabled = no-op.
    let (plugin_shutdown_tx, plugin_shutdown_rx) = tokio::sync::watch::channel(false);
    match payout_plugins::registry::PayoutPluginRegistry::from_env(
        std::path::PathBuf::from(&config.store.path)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join("payout-ledger.jsonl"),
    ) {
        Ok(registry) if !registry.is_empty() => {
            let payout_loop_registry = std::sync::Arc::new(registry);
            let shutdown_rx = plugin_shutdown_rx.clone();
            tokio::spawn({
                let payout_loop_registry = payout_loop_registry.clone();
                async move {
                    payout_loop_registry
                        .run_payout_loop(Duration::from_secs(60), shutdown_rx)
                        .await;
                }
            });
            // Wire confirmed share events into the plugins: every
            // MonitoringEvent::Share emitted by the organise worker
            // (post-promotion of a confirmed share) becomes a
            // PluginContext with the per-miner distribution taken from
            // the PPLNS window at that share's difficulty.
            let block_registry = payout_loop_registry;
            let network_for_payouts = stratum_config.network;
            let payout_window = plugin_pplns_window;
            tokio::spawn(async move {
                let mut event_rx = plugin_event_rx;
                loop {
                    match event_rx.recv().await {
                        Ok(p2poolv2_lib::monitoring_events::MonitoringEvent::Share(share)) => {
                            let miner_payouts = plugin_distribution_from_window(
                                &share,
                                &payout_window,
                                network_for_payouts,
                            );
                            let ctx = payout_plugins::PluginContext {
                                block_height: share.height,
                                block_hash: share.blockhash.to_string(),
                                block_reward_sats: plugin_block_reward_sats(share.height),
                                miner_payouts,
                            };
                            block_registry.on_block(&ctx);
                        }
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Payout plugin event stream lagged, skipped {n} events");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
            });
            info!("Payout plugins active");
        }
        Ok(_) => info!("No payout plugins configured"),
        // A ledger open failure with payout plugins configured is a
        // payment outage, not a warning: balances stop accruing and
        // miners stop being paid. Log at error and record it in the
        // metrics surface so dashboards see it.
        Err(e) => {
            error!("Payout plugins unavailable — configured plugins will NOT pay out: {e}");
            let metrics = metrics_for_shutdown.get_metrics().await;
            error!(
                payout_plugin_status = "unavailable",
                payout_plugin_error = %e,
                total_users = metrics.users.len(),
                "payout outage: plugin ledger failed to open"
            );
        }
    }

    let mut exit_receiver = exit_sender.subscribe();
    let stop_all = async move |reason: ShutdownReason| -> ShutdownReason {
        info!("Node shutting down...");

        // Save metrics before shutdown to prevent data loss
        let metrics = metrics_for_shutdown.get_metrics().await;
        if let Err(e) = p2poolv2_lib::accounting::stats::pool_local_stats::save_pool_local_stats(
            &metrics,
            &stats_dir_for_shutdown,
        ) {
            error!("Failed to save metrics on shutdown: {e}");
        } else {
            info!("Metrics saved on shutdown");
        }

        // Shutdown node gracefully
        if let Err(e) = node_handle.shutdown().await {
            error!("Failed to shutdown node: {e}");
        }

        // channels might be closed already, ignore errors
        let _ = stratum_shutdown_tx.send(());
        let _ = api_shutdown_tx.send(());
        // Stop the payout plugin loop before exiting.
        let _ = plugin_shutdown_tx.send(true);
        // Notify signal handler to exit
        let _ = exit_sender.send(reason);
        reason
    };

    // Check if shutdown was already requested before we started waiting
    let early_reason = *exit_receiver.borrow();
    if early_reason != ShutdownReason::None {
        stop_all(early_reason).await;
        trace!("Waiting signal handlers");
        sig_handle.await.unwrap();
        return if early_reason == ShutdownReason::Signal {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let shutdown_reason = tokio::select! {
        _ = stopping_rx => {
            // Node stopped unexpectedly - treat as error
            stop_all(ShutdownReason::Error).await
        },
        _ = exit_receiver.changed() => {
            let reason = *exit_receiver.borrow();
            stop_all(reason).await
        },
    };

    trace!("Waiting signal handlers");
    sig_handle.await.unwrap();

    if shutdown_reason == ShutdownReason::Signal {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Bitcoin block subsidy in sats for `height` (50 BTC halving
/// schedule). The pool's out-of-band plugin payouts mirror the
/// on-chain coinbase subsidy; fees accrue to the block finder.
fn plugin_block_reward_sats(height: u32) -> u64 {
    const HALVING_INTERVAL: u64 = 210_000;
    const INITIAL_SUBSIDY_SATS: u64 = 5_000_000_000;
    let halvings = (height as u64) / HALVING_INTERVAL;
    if halvings >= 64 {
        0
    } else {
        INITIAL_SUBSIDY_SATS >> halvings
    }
}

/// Per-miner sats for one confirmed share, taken from the PPLNS
/// window's difficulty distribution at the time of confirmation.
///
/// The share's own difficulty determines the window slice
/// (`get_distribution` walks entries up to that cumulative difficulty),
/// and each miner's proportional slice of the subsidy is computed the
/// same way the coinbase `append_proportional_distribution` does:
/// difficulty-weighted, deterministic remainder assignment.
fn plugin_distribution_from_window(
    share: &p2poolv2_lib::store::dag_store::ShareInfo,
    payout_window: &std::sync::Arc<
        std::sync::RwLock<p2poolv2_lib::accounting::payout::sharechain_pplns::PplnsWindow>,
    >,
    network: bitcoin::Network,
) -> Vec<payout_plugins::MinerBalance> {
    // The confirmed share's own difficulty is the PPLNS threshold for
    // this distribution.
    let share_difficulty = bitcoin::Target::from_compact(share.bits).difficulty(network);

    let Ok(mut window) = payout_window.write() else {
        error!("PPLNS window lock poisoned — skipping plugin accrual");
        return Vec::new();
    };
    let distribution = window.get_distribution(share_difficulty);
    drop(window);

    let total_difficulty: u128 = distribution.values().sum();
    if total_difficulty == 0 {
        return Vec::new();
    }
    let reward = plugin_block_reward_sats(share.height);
    // Deterministic proportional split, remainder to the
    // lexicographically-last address (matches the coinbase builder).
    let mut entries: Vec<(&bitcoin::Address, &u128)> = distribution.iter().collect();
    entries.sort_by_key(|(a, _)| a.to_string());
    let mut allocated: u64 = 0;
    let count = entries.len();
    let mut payouts = Vec::with_capacity(count);
    for (index, (address, difficulty)) in entries.into_iter().enumerate() {
        let sats = if index == count - 1 {
            reward.saturating_sub(allocated)
        } else {
            let s = ((reward as u128 * difficulty) / total_difficulty) as u64;
            allocated = allocated.saturating_add(s);
            s
        };
        if sats > 0 {
            payouts.push(payout_plugins::MinerBalance {
                miner_id: address.to_string(),
                sats,
            });
        }
    }
    payouts
}
