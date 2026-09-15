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

use crate::payout_plugins::ledger::{Ledger, to_miner_balances};
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
        let threshold = std::env::var("HYDRA_FEDIMINT_THRESHOLD_SATS")
            .ok()
            .and_then(|v| v.parse().ok());
        let destinations = std::env::var("HYDRA_FEDIMINT_DESTINATIONS").ok();
        let invite_code = std::env::var("HYDRA_FEDIMINT_INVITE_CODE").ok();
        Self::from_config(ledger, &gateway_api, threshold, destinations, invite_code)
    }

    /// Build from explicit config values (env in production, direct
    /// values in tests). See `from_env` for the env semantics.
    fn from_config(
        ledger: Arc<Ledger>,
        gateway_api: &str,
        threshold: Option<u64>,
        destinations: Option<String>,
        invite_code: Option<String>,
    ) -> Result<Self, PluginError> {
        if gateway_api.is_empty() {
            return Err(PluginError::Other(
                "HYDRA_FEDIMINT_GATEWAY_API not set — Fedimint plugin disabled".into(),
            ));
        }
        // A set-but-unparseable destinations map is a config error, not
        // an empty map: silently treating it as "nobody gets paid"
        // accrues balances forever with zero warning.
        let destinations = match destinations {
            None => HashMap::new(),
            Some(ref v) if v.trim().is_empty() => HashMap::new(),
            Some(v) => match serde_json::from_str(&v) {
                Ok(map) => map,
                Err(e) => {
                    return Err(PluginError::Other(format!(
                        "HYDRA_FEDIMINT_DESTINATIONS is set but not valid JSON: {e}"
                    )));
                }
            },
        };
        Ok(Self {
            ledger,
            gateway_api: gateway_api.to_string(),
            invite_code,
            threshold_sats: threshold.unwrap_or(10_000),
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
            if let Err(e) =
                self.ledger
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
            // Crash-safe ordering: debit + pending intent BEFORE the
            // gateway call; settle on success, rollback if the gateway
            // never accepted the payment.
            if let Err(e) = self
                .ledger
                .begin_payout("fedimint", &balance.miner_id, balance.sats)
            {
                tracing::error!(miner = %balance.miner_id, "Ledger begin_payout failed: {e}");
                continue;
            }
            match self.pay_via_gateway(&balance.miner_id, balance.sats).await {
                Ok(op_id) => {
                    match self.ledger.settle_payout(
                        "fedimint",
                        &balance.miner_id,
                        balance.sats,
                        &op_id,
                    ) {
                        Ok(_) => {
                            tracing::info!(miner = %balance.miner_id, sats = balance.sats, %op_id, "Fedimint payout");
                            paid.push(balance);
                        }
                        Err(e) => {
                            tracing::error!(miner = %balance.miner_id, "Ledger settle failed: {e}")
                        }
                    }
                }
                Err(e) => {
                    if let Err(re) =
                        self.ledger
                            .rollback_payout("fedimint", &balance.miner_id, balance.sats)
                    {
                        tracing::error!(miner = %balance.miner_id, "Ledger rollback failed: {re}");
                    }
                    tracing::warn!(miner = %balance.miner_id, "Fedimint payout failed, will retry: {e}");
                }
            }
        }
        Ok(paid)
    }

    fn balances(&self) -> HashMap<String, u64> {
        self.ledger.balances("fedimint")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_ledger() -> Arc<Ledger> {
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-fedimint-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Arc::new(Ledger::open(dir.join("ledger.jsonl")).unwrap())
    }

    fn plugin_with(
        ledger: Arc<Ledger>,
        gateway_api: String,
        dests: HashMap<String, String>,
    ) -> FedimintPlugin {
        FedimintPlugin {
            ledger,
            gateway_api,
            invite_code: None,
            threshold_sats: 0,
            destinations: dests,
            client: reqwest::Client::new(),
        }
    }

    #[test]
    fn malformed_destinations_env_is_a_hard_error() {
        let res = FedimintPlugin::from_config(
            tmp_ledger(),
            "https://gw.example.com",
            None,
            Some("nope:".to_string()),
            None,
        );
        match res {
            Err(PluginError::Other(m)) => assert!(m.contains("HYDRA_FEDIMINT_DESTINATIONS")),
            Err(other) => panic!("expected config error, got {other}"),
            Ok(_) => panic!("expected config error, got Ok"),
        }
    }

    #[tokio::test]
    async fn payout_due_happy_path_pays_once() {
        const MINER: &str = "fed1";
        const SATS: u64 = 20_000;
        let server = wiremock::MockServer::start().await;
        let ledger = tmp_ledger();
        ledger.accrue("fedimint", MINER, SATS, 1).unwrap();
        let mut dests = HashMap::new();
        dests.insert(MINER.to_string(), "op-dest-1".to_string());
        let plugin = plugin_with(ledger.clone(), server.uri(), dests);

        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/credit"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "operation_id": "op-77" })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let paid = plugin.payout_due().await.unwrap();
        assert_eq!(paid.len(), 1);
        assert_eq!(paid[0].sats, SATS);
        assert_eq!(ledger.balances("fedimint").get(MINER), None);
        // Second pass: nothing accrued, no second gateway call (the
        // .expect(1) above verifies exactly one credit).
        let paid2 = plugin.payout_due().await.unwrap();
        assert!(paid2.is_empty());
    }

    #[tokio::test]
    async fn payout_due_gateway_failure_restores_balance() {
        const MINER: &str = "fedfail";
        const SATS: u64 = 20_000;
        let server = wiremock::MockServer::start().await;
        let ledger = tmp_ledger();
        ledger.accrue("fedimint", MINER, SATS, 1).unwrap();
        let mut dests = HashMap::new();
        dests.insert(MINER.to_string(), "op-dest-2".to_string());
        let plugin = plugin_with(ledger.clone(), server.uri(), dests);

        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/credit"))
            .respond_with(wiremock::ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let paid = plugin.payout_due().await.unwrap();
        assert!(paid.is_empty());
        // Balance intact for retry.
        assert_eq!(ledger.balances("fedimint").get(MINER), Some(&SATS));
    }

    #[tokio::test]
    async fn payout_due_unmapped_miner_holds_balance() {
        const MINER: &str = "fedhold";
        const SATS: u64 = 20_000;
        let ledger = tmp_ledger();
        ledger.accrue("fedimint", MINER, SATS, 1).unwrap();
        let plugin = plugin_with(ledger, "https://gw.invalid".into(), HashMap::new());
        let paid = plugin.payout_due().await.unwrap();
        assert!(paid.is_empty());
        assert_eq!(plugin.balances().get(MINER), Some(&SATS));
    }
}
