//! Fedimint payout plugin.
//!
//! Pays accrued balances into a Fedimint federation on behalf of the
//! miner, delivered as a federation invite code + ecash note, or
//! directly into the miner's fedimint client via gateway API.
//!
//! Config (env):
//! - `HYDRA_FEDIMINT_INVITE_CODE` — federation the pool pays from
//! - `HYDRA_FEDIMINT_GATEWAY_API` — gateway REST/gRPC endpoint
//! - `HYDRA_FEDIMINT_THRESHOLD_SATS` — default 10_000
//! - `HYDRA_FEDIMINT_DESTINATIONS` — JSON map `{"username": "<fedimint
//!   receive invite / gateway op id>"}`; unmapped usernames accrue.
//!
//! NOTE: fedimint client APIs move quickly; this module targets the
//! gateway API surface and leaves direct federation client calls to
//! the integration pass.

use crate::payout_plugins::ledger::{to_miner_balances, Ledger};
use crate::payout_plugins::traits::{MinerBalance, PayoutPlugin, PluginContext, PluginError};
use std::collections::HashMap;
use std::sync::Arc;

pub struct FedimintPlugin {
    ledger: Arc<Ledger>,
    gateway_api: String,
    invite_code: Option<String>,
    threshold_sats: u64,
    destinations: HashMap<String, String>,
    client: reqwest::Client,
}

impl FedimintPlugin {
    pub fn from_env(ledger: Arc<Ledger>) -> Result<Self, PluginError> {
        let gateway_api = std::env::var("HYDRA_FEDIMINT_GATEWAY_API").unwrap_or_default();
        if gateway_api.is_empty() {
            return Err(PluginError::Other(
                "HYDRA_FEDIMINT_GATEWAY_API not set — Fedimint plugin disabled".into(),
            ));
        }
        let threshold = std::env::var("HYDRA_FEDIMINT_THRESHOLD_SATS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);
        let destinations = std::env::var("HYDRA_FEDIMINT_DESTINATIONS")
            .ok()
            .and_then(|v| serde_json::from_str(&v).ok())
            .unwrap_or_default();
        Ok(Self {
            ledger,
            gateway_api,
            invite_code: std::env::var("HYDRA_FEDIMINT_INVITE_CODE").ok(),
            threshold_sats: threshold,
            destinations,
            client: reqwest::Client::new(),
        })
    }

    /// Create an invoice at the gateway for `sats`, then pay it from
    /// the pool's federation wallet. The gateway hands the ecash /
    /// credit to the destination op the miner registered.
    async fn pay_via_gateway(&self, miner_id: &str, sats: u64) -> Result<String, PluginError> {
        let dest = self.destinations.get(miner_id).ok_or_else(|| {
            PluginError::NoDestination(format!("fedimint destination for {miner_id}"))
        })?;
        let resp = self
            .client
            .post(format!("{}/credit", self.gateway_api))
            .json(&serde_json::json!({
                "destination": dest,
                "amount_msat": sats * 1000,
                "invite_code": self.invite_code,
            }))
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| PluginError::Backend(format!("fedimint gateway: {e}")))?;
        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| PluginError::Backend(format!("fedimint json: {e}")))?;
        if !status.is_success() {
            return Err(PluginError::Backend(format!(
                "fedimint gateway {status}: {body}"
            )));
        }
        body.get("operation_id")
            .and_then(|o| o.as_str())
            .map(str::to_string)
            .ok_or_else(|| PluginError::Backend("gateway response missing operation_id".into()))
    }
}

#[async_trait::async_trait]
impl PayoutPlugin for FedimintPlugin {
    fn name(&self) -> &'static str {
        "fedimint"
    }

    fn on_block(&self, ctx: &PluginContext) {
        for payout in &ctx.miner_payouts {
            if let Err(e) = self
                .ledger
                .accrue("fedimint", &payout.miner_id, payout.sats, ctx.block_height)
            {
                tracing::error!(miner = %payout.miner_id, "Fedimint accrue failed: {e}");
            }
        }
    }

    fn threshold_sats(&self) -> u64 {
        self.threshold_sats
    }

    async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError> {
        let balances = self.ledger.balances("fedimint");
        let due: Vec<MinerBalance> = to_miner_balances(balances)
            .into_iter()
            .filter(|b| b.sats >= self.threshold_sats)
            .collect();
        let mut paid = Vec::new();
        for balance in due {
            if !self.destinations.contains_key(&balance.miner_id) {
                tracing::debug!(miner = %balance.miner_id, "No fedimint destination yet, holding");
                continue;
            }
            match self.pay_via_gateway(&balance.miner_id, balance.sats).await {
                Ok(op_id) => {
                    match self
                        .ledger
                        .record_payout("fedimint", &balance.miner_id, balance.sats)
                    {
                        Ok(_) => {
                            tracing::info!(miner = %balance.miner_id, sats = balance.sats, %op_id, "Fedimint payout");
                            paid.push(balance);
                        }
                        Err(e) => {
                            tracing::error!(miner = %balance.miner_id, "Ledger payout record failed: {e}")
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(miner = %balance.miner_id, "Fedimint payout failed, will retry: {e}");
                }
            }
        }
        Ok(paid)
    }

    async fn run(&self, _shutdown: tokio::sync::watch::Receiver<bool>) -> Result<(), PluginError> {
        Ok(())
    }

    fn balances(&self) -> HashMap<String, u64> {
        self.ledger.balances("fedimint")
    }
}
