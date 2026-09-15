//! Lightning payout plugin.
//!
//! Pays miner accruals to BOLT11 invoices fetched from a per-miner
//! pay-to endpoint (LNURL-pay style), or to static BOLT11 invoices
//! registered in the destination map.
//!
//! Config (env):
//! - `HYDRA_LN_API_URL`   — e.g. LND REST `https://127.0.0.1:8080`
//! - `HYDRA_LN_API_MACAROON` — hex macaroon for the pool's LN node
//! - `HYDRA_LN_THRESHOLD_SATS` — payout threshold (default 50_000)
//!
//! Destination resolution: miner stratum username may be
//! - an LNURL (lnurl1...) or LNURL-pay Lightning Address (user@host),
//!   resolved via the LNURL spec to a pay endpoint;
//! - anything else is looked up in `HYDRA_LN_DESTINATIONS` JSON map
//!   (`{"username": "user@host"}`); unmapped usernames accrue until
//!   mapped (credit is never lost).

use crate::payout_plugins::ledger::{to_miner_balances, Ledger};
use crate::payout_plugins::traits::{MinerBalance, PayoutPlugin, PluginContext, PluginError};
use std::collections::HashMap;
use std::sync::Arc;

pub struct LightningPlugin {
    ledger: Arc<Ledger>,
    api_url: String,
    macaroon: Option<String>,
    threshold_sats: u64,
    static_destinations: HashMap<String, String>,
    client: reqwest::Client,
}

impl LightningPlugin {
    pub fn from_env(ledger: Arc<Ledger>) -> Result<Self, PluginError> {
        let api_url = std::env::var("HYDRA_LN_API_URL").unwrap_or_default();
        if api_url.is_empty() {
            return Err(PluginError::Other(
                "HYDRA_LN_API_URL not set — Lightning plugin disabled".into(),
            ));
        }
        let threshold = std::env::var("HYDRA_LN_THRESHOLD_SATS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50_000);
        let static_destinations = std::env::var("HYDRA_LN_DESTINATIONS")
            .ok()
            .and_then(|v| serde_json::from_str(&v).ok())
            .unwrap_or_default();
        Ok(Self {
            ledger,
            api_url,
            macaroon: std::env::var("HYDRA_LN_API_MACAROON").ok(),
            threshold_sats: threshold,
            static_destinations,
            client: reqwest::Client::new(),
        })
    }

    /// Resolve a stratum username to an LNURL-pay endpoint.
    fn resolve_endpoint(&self, miner_id: &str) -> Option<String> {
        if let Some(dest) = self.static_destinations.get(miner_id) {
            return lnurl_pay_endpoint(dest);
        }
        lnurl_pay_endpoint(miner_id)
    }

    async fn fetch_invoice(
        &self,
        endpoint: &str,
        amount_msat: u64,
    ) -> Result<String, PluginError> {
        let url = if endpoint.contains('?') {
            format!("{endpoint}&amount={amount_msat}")
        } else {
            format!("{endpoint}?amount={amount_msat}")
        };
        let resp = self
            .client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| PluginError::Backend(format!("lnurl fetch: {e}")))?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| PluginError::Backend(format!("lnurl json: {e}")))?;
        // LNURL-pay response: {pr: "<bolt11>", status: "OK"}
        if body.get("status").and_then(|s| s.as_str()) != Some("OK") {
            return Err(PluginError::Backend(format!(
                "lnurl error: {body}"
            )));
        }
        body.get("pr")
            .and_then(|p| p.as_str())
            .map(str::to_string)
            .ok_or_else(|| PluginError::Backend("lnurl response missing pr".into()))
    }

    /// Pay a BOLT11 invoice through the configured LN node (LND REST).
    async fn pay_invoice(&self, invoice: &str) -> Result<String, PluginError> {
        let mut req = self
            .client
            .post(format!("{}/v1/channels/transactions", self.api_url))
            .json(&serde_json::json!({ "payment_request": invoice }))
            .timeout(std::time::Duration::from_secs(60));
        if let Some(mac) = &self.macaroon {
            req = req.header("Grpc-Metadata-macaroon", mac);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| PluginError::Backend(format!("lnd pay: {e}")))?;
        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| PluginError::Backend(format!("lnd pay json: {e}")))?;
        if !status.is_success() {
            return Err(PluginError::Backend(format!(
                "lnd pay {}: {}",
                status, body
            )));
        }
        // LND returns payment_hash + status; SUCCEEDED means settled.
        match body.get("status").and_then(|s| s.as_str()) {
            Some("SUCCEEDED") => Ok(body
                .get("payment_hash")
                .and_then(|h| h.as_str())
                .unwrap_or("")
                .to_string()),
            other => Err(PluginError::Backend(format!(
                "lnd payment not settled: {other:?}"
            ))),
        }
    }
}

/// Detect Lightning Address (user@host) or https LNURL-pay callback
/// and return the pay endpoint, per the LNURL spec.
fn lnurl_pay_endpoint(dest: &str) -> Option<String> {
    if let Some((user, host)) = dest.split_once('@') {
        if !host.contains('.') {
            return None;
        }
        Some(format!("https://{host}/.well-known/lnurlp/{user}"))
    } else if dest.starts_with("lnurl") {
        // bech32-decoded lnurl — decode via lightning-invoice style.
        // Minimal handling: require https:// form for now; bech32
        // decode lands with the bech32 dep in the next pass.
        None
    } else {
        None
    }
}

#[async_trait::async_trait]
impl PayoutPlugin for LightningPlugin {
    fn name(&self) -> &'static str {
        "lightning"
    }

    fn on_block(&self, ctx: &PluginContext) {
        for payout in &ctx.miner_payouts {
            if let Err(e) = self.ledger.accrue("lightning", &payout.miner_id, payout.sats, ctx.block_height) {
                tracing::error!(miner = %payout.miner_id, "Lightning accrue failed: {e}");
            }
        }
    }

    fn threshold_sats(&self) -> u64 {
        self.threshold_sats
    }

    async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError> {
        let balances = self.ledger.balances("lightning");
        let due: Vec<MinerBalance> = to_miner_balances(balances)
            .into_iter()
            .filter(|b| b.sats >= self.threshold_sats)
            .collect();
        let mut paid = Vec::new();
        for balance in due {
            let Some(endpoint) = self.resolve_endpoint(&balance.miner_id) else {
                tracing::debug!(miner = %balance.miner_id, "No LN destination yet, holding");
                continue;
            };
            let msat = balance.sats * 1000;
            match self.fetch_invoice(&endpoint, msat).await {
                Ok(inv) => match self.pay_invoice(&inv).await {
                Ok(hash) => {
                    match self.ledger.record_payout("lightning", &balance.miner_id, balance.sats) {
                        Ok(remaining) => {
                            tracing::info!(miner = %balance.miner_id, sats = balance.sats, %hash, "LN payout");
                            paid.push(balance);
                            let _ = remaining;
                        }
                        Err(e) => tracing::error!(miner = %balance.miner_id, "Ledger payout record failed: {e}"),
                    }
                }
                Err(e) => {
                    // Balance stays accrued; next pass retries.
                    tracing::warn!(miner = %balance.miner_id, "LN payout failed, will retry: {e}");
                }
                },
                Err(e) => {
                    tracing::warn!(miner = %balance.miner_id, "LN invoice fetch failed, will retry: {e}");
                }
            }
        }
        Ok(paid)
    }

    async fn run(&self, _shutdown: tokio::sync::watch::Receiver<bool>) -> Result<(), PluginError> {
        // Future: subscribe LND invoice settle events for exact
        // reconciliation. The payout loop in the registry drives us now.
        Ok(())
    }

    fn balances(&self) -> HashMap<String, u64> {
        self.ledger.balances("lightning")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lightning_address_resolves_to_well_known() {
        let ep = lnurl_pay_endpoint("miner@example.com").unwrap();
        assert_eq!(ep, "https://example.com/.well-known/lnurlp/miner");
    }

    #[test]
    fn non_ln_usernames_do_not_resolve() {
        assert!(lnurl_pay_endpoint("rig1").is_none());
    }

    #[test]
    fn bech32_lnurl_deferred() {
        // Explicitly unimplemented until bech32 dep lands.
        assert!(lnurl_pay_endpoint("lnurl1dp68gurn8ghj7").is_none());
    }
}
