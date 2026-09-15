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
use std::collections::HashMap;
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
    let config = match Config::load(&args.config) {
        Ok(config) => config,
        Err(err) => {
            error!("Failed to load config: {err}");
            return ExitCode::FAILURE;
        }
    };
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
    // Template tap: every NotifyCmd::SendToAll from gbt carries the
    // block template the miners are being told to work on. The tap
    // records the latest template (the payout plugins distribute the
    // coinbase of the block actually mined from it) and forwards the
    // command to the notifier unchanged.
    let (notify_tx_in, notify_tap_rx) = tokio::sync::mpsc::channel(NOTIFY_CHANNEL_CAPACITY);
    let (notify_tx, notify_rx) = tokio::sync::mpsc::channel(NOTIFY_CHANNEL_CAPACITY);
    let (latest_template_tx, latest_template_rx) = tokio::sync::watch::channel(
        None::<std::sync::Arc<p2poolv2_lib::stratum::work::block_template::BlockTemplate>>,
    );
    {
        let latest_template_tx = latest_template_tx.clone();
        tokio::spawn(async move {
            let mut tap_rx = notify_tap_rx;
            while let Some(cmd) = tap_rx.recv().await {
                if let p2poolv2_lib::stratum::work::notify::NotifyCmd::SendToAll { template } = &cmd
                {
                    let _ = latest_template_tx.send(Some(std::sync::Arc::clone(template)));
                }
                // The notifier task shutting down its receiver ends the
                // tap; nothing else consumes the stream.
                if notify_tx.send(cmd).await.is_err() {
                    break;
                }
            }
        });
    }
    let tracker_handle = start_tracker_actor();

    let notify_tx_for_gbt = notify_tx_in.clone();
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
        let gbt_result = start_gbt(
            bitcoinrpc_config_cloned,
            notify_tx_for_gbt,
            GBT_POLL_INTERVAL,
            stratum_config.network,
            zmq_trigger_rx,
        )
        .await;
        if let Err(e) = gbt_result
            && *exit_receiver_gbt.borrow() == ShutdownReason::None
        {
            tracing::error!("Failed to fetch block template. Shutting down. \n {e}");
            let _ = exit_sender_gbt.send(ShutdownReason::Error);
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
    let notify_tx_for_node = notify_tx_in.clone();
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
                notify_tx_in,
                tracker_handle_cloned,
                bitcoinrpc_config,
                metrics_cloned,
                template_rx,
            )
            .await;
        if let Err(e) = result
            && *exit_receiver_stratum.borrow() == ShutdownReason::None
        {
            error!("Failed to start Stratum server: {e}");
            let _ = exit_sender_stratum.send(ShutdownReason::Error);
        }
        info!("Stratum server stopped");
    });

    let (monitoring_event_sender, _monitoring_event_receiver) =
        p2poolv2_lib::monitoring_events::create_monitoring_event_channel();

    let pool_signature_for_api = stratum_config.pool_signature.clone();
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
        pool_signature_for_api,
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
            // Wire block-found events into the plugins. bitcoind's
            // zmqpubhashblock fires for EVERY block accepted by the
            // network, so the raw block hash is kept (the lib's
            // ZmqListener discards it), the block is fetched over RPC,
            // and accrual is gated on the block actually being one of
            // ours: its coinbase ends with the pool signature and its
            // outputs sum to the template's coinbase value. Each
            // pool-found block then distributes the miner-attributable
            // coinbase (coinbasevalue minus donation/fee cuts, the same
            // cuts the coinbase builder applied) over the PPLNS window
            // using the same difficulty threshold the coinbase builder
            // used (network difficulty of the mined template's bits
            // times the configured difficulty multiplier). Accruing per
            // confirmed share would instead credit a full subsidy ~60
            // times per bitcoin block and drain the pool's LN node.
            let block_registry = payout_loop_registry;
            let network_for_payouts = stratum_config.network;
            let difficulty_multiplier_for_payouts = stratum_config.difficulty_multiplier as u128;
            let payout_window = plugin_pplns_window;
            let chain_store_for_payouts = chain_store_handle;
            let rpc_for_payouts = match bitcoindrpc::BitcoindRpcClient::new(
                &config.bitcoinrpc.url,
                &config.bitcoinrpc.username,
                &config.bitcoinrpc.password,
            ) {
                Ok(client) => client,
                Err(e) => {
                    error!("Failed to create bitcoind RPC client for payout plugins: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let pool_signature_for_payouts = stratum_config
                .pool_signature
                .clone()
                .map(String::into_bytes);
            let donation_for_payouts = stratum_config.donation;
            let donation_address_for_payouts = stratum_config.donation_address_parsed.clone();
            let fee_for_payouts = stratum_config.fee;
            let fee_address_for_payouts = stratum_config.fee_address_parsed.clone();
            let zmq_rx_for_payouts = match payout_plugins::block_found::start_hashblock_listener(
                &stratum_config.zmqpubhashblock,
            ) {
                Ok(rx) => rx,
                Err(e) => {
                    error!("Failed to set up ZMQ listener for payout plugins: {e}");
                    return ExitCode::FAILURE;
                }
            };
            tokio::spawn(async move {
                let mut zmq_rx = zmq_rx_for_payouts;
                let mut latest_template_rx = latest_template_rx;
                // Recent templates and recently-accrued block hashes:
                // the template history absorbs the race where gbt
                // publishes a new template before the accrual task sees
                // the block mined from the previous one; the hash ring
                // dedups duplicate zmq notifications for one block.
                let mut recent_templates = std::collections::VecDeque::new();
                let mut accrued_blocks = std::collections::VecDeque::new();
                while let Some(hash_bytes) = zmq_rx.recv().await {
                    // Take any newer template the notifier published —
                    // but keep the old ones, the found block may have
                    // been mined from any of them.
                    while latest_template_rx.has_changed().unwrap_or(false) {
                        if let Some(t) = latest_template_rx.borrow_and_update().clone() {
                            payout_plugins::block_found::remember_template(
                                &mut recent_templates,
                                t,
                            );
                        }
                    }
                    if recent_templates.is_empty() {
                        warn!(
                            "Block announced before any block template known — skipping plugin accrual"
                        );
                        continue;
                    }
                    let Some(block) = payout_plugins::block_found::fetch_block_by_hash(
                        &rpc_for_payouts,
                        &hash_bytes,
                    )
                    .await
                    else {
                        // Not fetchable (pruned, lagging node) — skip;
                        // accrual only ever happens on a verified block.
                        continue;
                    };
                    let block_hash = block.header.block_hash();
                    if !payout_plugins::block_found::accrue_once(&mut accrued_blocks, block_hash) {
                        continue;
                    }
                    let matched = recent_templates.iter().rev().find(|template| {
                        payout_plugins::block_found::is_pool_block(
                            &block,
                            template,
                            pool_signature_for_payouts.as_deref(),
                        )
                    });
                    let Some(template) = matched else {
                        trace!(
                            height = block.header.block_hash().to_string().as_str(),
                            "Network block is not a pool block — no plugin accrual"
                        );
                        continue;
                    };
                    // The found block's coinbase was built from this
                    // template: its outputs already carry the donation
                    // and fee cuts, so accrue only the miner share.
                    let reward_sats = payout_plugins::block_found::miner_attributable_sats(
                        template.coinbasevalue,
                        donation_for_payouts,
                        donation_address_for_payouts.as_ref(),
                        fee_for_payouts,
                        fee_address_for_payouts.as_ref(),
                    );
                    if reward_sats == 0 {
                        continue; // 100% donation/fee — miners accrue nothing
                    }
                    let Ok(compact) =
                        bitcoin::pow::CompactTarget::from_unprefixed_hex(&template.bits)
                    else {
                        error!(
                            "Payout plugins: template bits '{}' unparseable — skipping accrual",
                            template.bits
                        );
                        continue;
                    };
                    let total_difficulty = bitcoin::Target::from_compact(compact)
                        .difficulty(network_for_payouts)
                        .saturating_mul(difficulty_multiplier_for_payouts);
                    let miner_payouts = plugin_distribution_from_window(
                        &payout_window,
                        &chain_store_for_payouts,
                        total_difficulty,
                        reward_sats,
                    );
                    let ctx = payout_plugins::PluginContext {
                        block_height: template.height,
                        miner_payouts,
                    };
                    block_registry.on_block(&ctx);
                    info!(
                        height = template.height,
                        block_hash = %block_hash,
                        reward_sats,
                        "Pool block found — plugin accrual complete"
                    );
                }
            });
            info!("Payout plugins active (pool-block-gated accrual via zmqpubhashblock)");
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

/// Per-miner sats for one pool-found bitcoin block, taken from the
/// PPLNS window at the same threshold the coinbase builder used.
///
/// `total_difficulty` must be the block's network difficulty times the
/// configured difficulty multiplier (exactly what
/// `build_output_distribution` in the notify worker passes to
/// `get_output_distribution`), and `reward_sats` the block's
/// miner-attributable coinbase (coinbasevalue after the donation/fee
/// cuts, see `block_found::miner_attributable_sats`) — so the plugin
/// accrual for one block equals the miner-attributable coinbase
/// outputs, once per pool block, not once per share or once per
/// network block. Each miner's proportional slice is computed the same
/// way the coinbase `append_proportional_distribution` does:
/// difficulty-weighted, deterministic remainder assignment.
fn plugin_distribution_from_window(
    payout_window: &std::sync::Arc<
        std::sync::RwLock<p2poolv2_lib::accounting::payout::sharechain_pplns::PplnsWindow>,
    >,
    chain_store_handle: &p2poolv2_lib::shares::chain::chain_store_handle::ChainStoreHandle,
    total_difficulty: u128,
    reward_sats: u64,
) -> Vec<payout_plugins::MinerBalance> {
    if total_difficulty == 0 {
        return Vec::new();
    }
    let Ok(mut window) = payout_window.write() else {
        error!("PPLNS window lock poisoned — skipping plugin accrual");
        return Vec::new();
    };
    // Pull newly confirmed shares into the window cache before
    // walking it (same contract as Payout::fill_distribution_from_shares).
    if let Err(e) = window.update(chain_store_handle) {
        error!("PPLNS window update failed — skipping plugin accrual: {e}");
        return Vec::new();
    }
    let distribution = window.get_distribution(total_difficulty);
    drop(window);

    proportional_split(&distribution, reward_sats)
}

/// Deterministic difficulty-weighted split of `reward_sats` across the
/// distribution's addresses, remainder to the lexicographically-last
/// address (matches the coinbase `append_proportional_distribution`).
/// The returned sats always sum to exactly `reward_sats` (when the
/// distribution is non-empty) — one block, one reward.
fn proportional_split(
    distribution: &HashMap<bitcoin::Address, u128>,
    reward_sats: u64,
) -> Vec<payout_plugins::MinerBalance> {
    let window_total: u128 = distribution.values().sum();
    if window_total == 0 {
        return Vec::new();
    }
    let mut entries: Vec<(&bitcoin::Address, &u128)> = distribution.iter().collect();
    entries.sort_by_key(|(a, _)| a.to_string());
    let mut allocated: u64 = 0;
    let count = entries.len();
    let mut payouts = Vec::with_capacity(count);
    for (index, (address, difficulty)) in entries.into_iter().enumerate() {
        let sats = if index == count - 1 {
            reward_sats.saturating_sub(allocated)
        } else {
            let s = ((reward_sats as u128 * difficulty) / window_total) as u64;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// The accrual for one block interval must equal the block reward
    /// exactly once — regardless of how many confirmed shares the
    /// window holds. Regression guard for the per-share over-credit
    /// bug (each confirmed share used to credit a full subsidy).
    #[test]
    fn distribution_sums_to_block_reward_once() {
        use bitcoin::CompressedPublicKey;
        use bitcoin::hashes::{Hash, sha256d};
        let network = bitcoin::Network::Signet;
        let addr = |pubkey_hex: &str| -> String {
            let pubkey: CompressedPublicKey = pubkey_hex.parse().unwrap();
            bitcoin::Address::p2wpkh(&pubkey, network).to_string()
        };
        let miner_a = addr("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798");
        let miner_b = addr("02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5");
        // Simulate a block interval of ~60 confirmed shares alternating
        // between two miners of equal difficulty.
        let mut window =
            p2poolv2_lib::accounting::payout::sharechain_pplns::PplnsWindow::new(network);
        let shares: Vec<_> = (0..60u64)
            .map(|i| {
                (
                    bitcoin::BlockHash::from_raw_hash(sha256d::Hash::from_byte_array([
                        (i % 256) as u8,
                        0x11,
                        0x22,
                        0x33,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    ])),
                    if i % 2 == 0 {
                        miner_a.clone()
                    } else {
                        miner_b.clone()
                    },
                    100u128,
                    vec![],
                )
            })
            .collect();
        window.populate_for_benchmark(shares);

        const REWARD: u64 = 3_125_000_000;
        // Threshold = whole window, exactly what a block's
        // network_difficulty * multiplier slice consumes.
        let distribution = window.get_distribution(u128::MAX);
        let payouts = proportional_split(&distribution, REWARD);
        let total: u64 = payouts.iter().map(|p| p.sats).sum();
        assert_eq!(
            total, REWARD,
            "one block interval must accrue exactly one block reward, not one reward per share"
        );
        assert_eq!(payouts.len(), 2);
        for p in &payouts {
            assert_eq!(p.sats, REWARD / 2, "equal difficulty splits evenly");
        }
    }

    /// The split is deterministic: same distribution, same result, and
    /// the remainder from integer truncation always lands on the same
    /// (lexicographically-last) address.
    #[test]
    fn proportional_split_remainder_is_deterministic() {
        let mk = |s: &str| -> bitcoin::Address {
            s.parse::<bitcoin::Address<_>>().unwrap().assume_checked()
        };
        let a = mk("bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq");
        let b = mk("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
        let c = mk("bc1q34aq5drpuwy3wgl9lhup9892qp6svr8ldzyy7c");
        // 3 addresses splitting 100 sats that cannot divide evenly:
        // 33 + 33 + 34.
        let distribution = HashMap::from([(a, 1u128), (b, 1u128), (c, 1u128)]);
        let mut first: Vec<(String, u64)> = proportional_split(&distribution, 100)
            .into_iter()
            .map(|p| (p.miner_id, p.sats))
            .collect();
        first.sort();
        for _ in 0..10 {
            let mut again: Vec<(String, u64)> = proportional_split(&distribution, 100)
                .into_iter()
                .map(|p| (p.miner_id, p.sats))
                .collect();
            again.sort();
            assert_eq!(again, first, "split must be deterministic");
        }
        assert_eq!(
            first.iter().map(|(_, sats)| sats).sum::<u64>(),
            100,
            "remainder must not be lost"
        );
        assert!(
            first.iter().any(|(_, sats)| *sats == 34),
            "the 1-sat remainder must land somewhere exactly"
        );
    }
}
