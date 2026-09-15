//! Cashu ecash payout plugin.
//!
//! Mints fresh ecash tokens for accrued balances via a Cashu mint's
//! REST API (NUT-04 mint quote flow) and delivers the token string to
//! the miner's registered delivery channel.
//!
//! Config (env):
//! - `HYDRA_CASHU_MINT_URL` — e.g. `https://mint.example.com`
//! - `HYDRA_CASHU_THRESHOLD_SATS` — payout threshold (default 10_000)
//! - `HYDRA_CASHU_DESTINATIONS` — JSON map `{"username":
//!   "<topic>:<topic access token>"}`; unmapped usernames accrue until
//!   mapped.
//!
//! Delivery requires an AUTHENTICATED, NON-PUBLIC ntfy topic. The
//! destination value encodes BOTH halves of that requirement as
//! `<topic>:<topic access token>`: the Bearer token authenticates the
//! PUBLISHER to ntfy, but on ntfy.sh anyone can still subscribe and
//! read a topic unless it is reserved with access control — so the
//! topic itself must be a secret (a reserved/ACL'd topic name, or a
//! random suffix the operator communicated to the miner out-of-band).
//! The bare guessable form `hydrapool-<username>` is REFUSED: bearer
//! ecash readable by anyone who guesses a public topic name is
//! theft-by-default, and an authenticated publisher does not fix that.
//!
//! Undelivered tokens are re-delivered, never re-minted: the minted
//! payload is attached to the still-open pending intent
//! ([`Ledger::attach_pending_payload`]) BEFORE delivery, the intent is
//! only settled once `deliver` succeeds, and every payout pass
//! re-attempts delivery for open intents that already carry a
//! `minted:` payload (including ones stranded by a restart).
//!
//! Token construction: NUT-04 blinded outputs require the cdk wallet
//! crate (blinded-message generation, keyset selection, NUT-00 token
//! serialization). Until that dependency lands this plugin refuses to
//! enable when a mint URL is configured WITHOUT `HYDRA_CASHU_ALLOW_NUT04_PROBE=true`.
//! In probe mode it exercises the quote/mint API but the delivered
//! payload is not a redeemable NUT-00 token — operators must not treat
//! probe-mode payouts as real payments, and the ledger records the
//! exact minted payload for reconciliation. Default (no opt-in): the
//! plugin stays disabled so balances accrue instead of being marked
//! paid against an unredeemable string.

use crate::payout_plugins::ledger::{Ledger, to_miner_balances};
use crate::payout_plugins::traits::{MinerBalance, PayoutPlugin, PluginContext, PluginError};
use std::collections::HashMap;
use std::sync::Arc;

pub struct CashuPlugin {
    ledger: Arc<Ledger>,
    mint_url: String,
    threshold_sats: u64,
    /// miner_id -> (secret ntfy topic, topic access token).
    destinations: HashMap<String, (String, String)>,
    client: reqwest::Client,
}

impl CashuPlugin {
    pub fn from_env(ledger: Arc<Ledger>) -> Result<Self, PluginError> {
        let mint_url = std::env::var("HYDRA_CASHU_MINT_URL").unwrap_or_default();
        let probe = std::env::var("HYDRA_CASHU_ALLOW_NUT04_PROBE")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let threshold = std::env::var("HYDRA_CASHU_THRESHOLD_SATS")
            .ok()
            .and_then(|v| v.parse().ok());
        let destinations = std::env::var("HYDRA_CASHU_DESTINATIONS").ok();
        Self::from_config(ledger, &mint_url, probe, threshold, destinations)
    }

    /// Build from explicit config values (env in production, direct
    /// values in tests). See `from_env` for the env semantics.
    fn from_config(
        ledger: Arc<Ledger>,
        mint_url: &str,
        probe: bool,
        threshold: Option<u64>,
        destinations: Option<String>,
    ) -> Result<Self, PluginError> {
        if mint_url.is_empty() {
            return Err(PluginError::Other(
                "HYDRA_CASHU_MINT_URL not set — Cashu plugin disabled".into(),
            ));
        }
        // The mint flow cannot yet produce a real NUT-00 token (blinded
        // outputs need the cdk crate). Unless the operator explicitly
        // opts into probe mode, refuse to enable: paying out an
        // unredeemable string would mark sats as paid while the miner
        // gets nothing.
        if !probe {
            return Err(PluginError::Other(
                "Cashu mint flow requires the cdk wallet integration (real blinded \
                 outputs / NUT-00 tokens). Set HYDRA_CASHU_ALLOW_NUT04_PROBE=true to \
                 run the API probe anyway — payouts are NOT redeemable in probe mode. \
                 Cashu plugin disabled."
                    .into(),
            ));
        }
        // A set-but-unparseable destinations map is a config error, not
        // an empty map: silently treating it as "nobody gets paid"
        // accrues balances forever with zero warning. Each entry must
        // be `<secret topic>:<topic access token>`; the bare public
        // topic `hydrapool-<username>` is refused (see module docs).
        let destinations = match destinations {
            None => HashMap::new(),
            Some(ref v) if v.trim().is_empty() => HashMap::new(),
            Some(v) => match parse_destinations(&v) {
                Ok(map) => map,
                Err(e) => {
                    return Err(PluginError::Other(format!(
                        "HYDRA_CASHU_DESTINATIONS is invalid: {e}"
                    )));
                }
            },
        };
        Ok(Self {
            ledger,
            mint_url: mint_url.trim_end_matches('/').to_string(),
            threshold_sats: threshold.unwrap_or(10_000),
            destinations,
            client: reqwest::Client::new(),
        })
    }

    /// NUT-04: create a mint quote, then request blind signatures.
    /// Returns the minted payload to hand to the miner.
    ///
    /// NOTE: `outputs` is a placeholder until the cdk crate lands; the
    /// mint will reject empty output lists or return signatures that
    /// cannot be redeemed. The returned value is the raw mint response,
    /// recorded to the ledger for reconciliation — it is NOT a
    /// spendable NUT-00 token.
    async fn mint_ecash(&self, sats: u64) -> Result<String, PluginError> {
        // Step 1: quote — POST /v1/mint/quote/bolt64
        let quote_resp = self
            .client
            .post(format!("{}/v1/mint/quote/bolt64", self.mint_url))
            .json(&serde_json::json!({ "amount": sats, "unit": "sat" }))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| PluginError::Backend(format!("cashu quote: {e}")))?;
        let quote: serde_json::Value = quote_resp
            .json()
            .await
            .map_err(|e| PluginError::Backend(format!("cashu quote json: {e}")))?;
        let quote_id = quote
            .get("quote")
            .and_then(|q| q.as_str())
            .ok_or_else(|| PluginError::Backend(format!("cashu quote missing id: {quote}")))?;

        // The pool pre-funds the mint with an E-Cash wallet balance;
        // this quote is settled from the pool's own mint balance, so no
        // Lightning payment step is needed. Step 2: mint the blinded
        // signatures against the quote.
        //
        // Blinded message construction (NUT-03/00) is delegated to the
        // mint wallet integration — the pool signs blank outputs per
        // the wallet's keyset. Placeholder payload until the cdk crate
        // is added; the API shape is stable either way.
        let mint_resp = self
            .client
            .post(format!("{}/v1/mint/bolt64", self.mint_url))
            .json(&serde_json::json!({
                "quote": quote_id,
                "outputs": []
            }))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| PluginError::Backend(format!("cashu mint: {e}")))?;
        if !mint_resp.status().is_success() {
            return Err(PluginError::Backend(format!(
                "cashu mint {}",
                mint_resp.status()
            )));
        }
        let signatures: serde_json::Value = mint_resp
            .json()
            .await
            .map_err(|e| PluginError::Backend(format!("cashu mint json: {e}")))?;

        // Persist the minted payload verbatim (token serialization /
        // keyset handling comes with the wallet layer).
        Ok(signatures.to_string())
    }

    /// Deliver an ecash payload to the miner's secret ntfy topic. The
    /// destination encodes `<topic>:<topic access token>`; the topic
    /// must be non-public (see module docs) and the Bearer token
    /// authenticates the publisher. Never delivers without an
    /// Authorization header.
    async fn deliver(&self, miner_id: &str, token: &str) -> Result<(), PluginError> {
        let (topic, topic_token) = self.destinations.get(miner_id).ok_or_else(|| {
            PluginError::NoDestination(format!("cashu destination for {miner_id}"))
        })?;
        if topic_token.trim().is_empty() {
            return Err(PluginError::Rejected(format!(
                "empty ntfy access token for {miner_id} — refusing unauthenticated delivery"
            )));
        }
        if topic.trim().is_empty() {
            return Err(PluginError::Rejected(format!(
                "empty ntfy topic for {miner_id} — refusing to guess a public topic name"
            )));
        }
        let resp = self
            .client
            .post(format!("https://ntfy.sh/{}", topic.trim()))
            .header("Authorization", format!("Bearer {}", topic_token.trim()))
            .body(token.to_string())
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| PluginError::Backend(format!("cashu deliver: {e}")))?;
        if !resp.status().is_success() {
            return Err(PluginError::Backend(format!(
                "cashu deliver {}",
                resp.status()
            )));
        }
        Ok(())
    }
}

/// Parse the destinations JSON map into (secret topic, access token)
/// pairs. Every value must be `<topic>:<access token>`; the bare
/// guessable topic `hydrapool-<username>` is refused because anyone
/// can subscribe to a public ntfy.sh topic and read the bearer ecash.
pub fn parse_destinations(raw: &str) -> Result<HashMap<String, (String, String)>, String> {
    let parsed: HashMap<String, String> =
        serde_json::from_str(raw).map_err(|e| format!("not valid JSON: {e}"))?;
    let mut out = HashMap::with_capacity(parsed.len());
    for (miner, value) in parsed {
        let (topic, token) = value
            .split_once(':')
            .ok_or_else(|| {
                format!(
                    "destination for {miner} must be `<topic>:<topic access token>`, got a value with no ':' separator"
                )
            })?;
        let topic = topic.trim();
        let token = token.trim();
        if topic.is_empty() || token.is_empty() {
            return Err(format!(
                "destination for {miner} has an empty topic or access token"
            ));
        }
        let bare_public = format!("hydrapool-{miner}");
        if topic == bare_public {
            return Err(format!(
                "destination for {miner} uses the bare public topic '{topic}' — anyone on ntfy.sh can subscribe to it and read the ecash. Use a reserved/ACL'd topic or a random secret suffix (e.g. 'hydrapool-{miner}-<random>') and give it to the miner out-of-band"
            ));
        }
        out.insert(miner, (topic.to_string(), token.to_string()));
    }
    Ok(out)
}

#[async_trait::async_trait]
impl PayoutPlugin for CashuPlugin {
    fn name(&self) -> &'static str {
        "cashu"
    }

    fn on_block(&self, ctx: &PluginContext) {
        for payout in &ctx.miner_payouts {
            if let Err(e) =
                self.ledger
                    .accrue("cashu", &payout.miner_id, payout.sats, ctx.block_height)
            {
                tracing::error!(miner = %payout.miner_id, "Cashu accrue failed: {e}");
            }
        }
    }

    fn threshold_sats(&self) -> u64 {
        self.threshold_sats
    }

    async fn payout_due(&self) -> Result<Vec<MinerBalance>, PluginError> {
        // Re-deliver minted-but-undelivered tokens first: an open
        // pending intent carrying a `minted:` payload means the token
        // already exists (the balance was debited and the mint paid
        // for) — re-attempt delivery from the stored payload instead
        // of minting anew. This also picks up intents stranded by a
        // restart, since pending_payouts replays the ledger file.
        let mut paid = Vec::new();
        for (plugin, miner_id, sats, pending) in self.ledger.pending_payouts() {
            if plugin != "cashu" {
                continue;
            }
            let Some(token) = pending.operation_id.strip_prefix("minted:") else {
                continue;
            };
            if token.is_empty() {
                continue;
            }
            match self.deliver(&miner_id, token).await {
                Ok(()) => {
                    // Delivery succeeded — only now close the intent.
                    if let Err(e) =
                        self.ledger
                            .settle_payout("cashu", &miner_id, sats, &pending.operation_id)
                    {
                        tracing::error!(miner = %miner_id, "Ledger settle of re-delivered token failed: {e}");
                        continue;
                    }
                    tracing::info!(miner = %miner_id, sats, "Cashu token re-delivered from ledger");
                    paid.push(MinerBalance { miner_id, sats });
                }
                Err(e) => {
                    tracing::warn!(miner = %miner_id, "Undelivered cashu token still stranded, will retry: {e}");
                }
            }
        }

        let balances = self.ledger.balances("cashu");
        let due: Vec<MinerBalance> = to_miner_balances(balances)
            .into_iter()
            .filter(|b| b.sats >= self.threshold_sats)
            .collect();
        for balance in due {
            if !self.destinations.contains_key(&balance.miner_id) {
                tracing::debug!(miner = %balance.miner_id, "No cashu destination yet, holding");
                continue;
            }
            // Crash-safe ordering: the ledger debit + pending intent is
            // written BEFORE the mint quote, so a crash mid-flow leaves
            // a recoverable pending entry instead of re-minting (which
            // would pay the mint twice for one accrual).
            if let Err(e) = self
                .ledger
                .begin_payout("cashu", &balance.miner_id, balance.sats)
            {
                tracing::error!(miner = %balance.miner_id, "Ledger begin_payout failed: {e}");
                continue;
            }
            match self.mint_ecash(balance.sats).await {
                Ok(token) => {
                    // Attach the minted payload to the STILL-OPEN intent
                    // before delivery: if delivery fails (or we crash),
                    // the payload is recoverable from the ledger and the
                    // re-delivery pass above picks it up. The intent is
                    // only settled once delivery succeeds.
                    if let Err(e) = self.ledger.attach_pending_payload(
                        "cashu",
                        &balance.miner_id,
                        &format!("minted:{token}"),
                    ) {
                        tracing::error!(miner = %balance.miner_id, "Ledger token attach failed: {e}");
                        continue;
                    }
                    match self.deliver(&balance.miner_id, &token).await {
                        Ok(()) => {
                            match self.ledger.settle_payout(
                                "cashu",
                                &balance.miner_id,
                                balance.sats,
                                &format!("minted:{token}"),
                            ) {
                                Ok(_) => {
                                    tracing::info!(miner = %balance.miner_id, sats = balance.sats, "Cashu payout delivered");
                                    paid.push(balance);
                                }
                                Err(e) => {
                                    tracing::error!(miner = %balance.miner_id, "Ledger settle failed after delivery: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            // Token minted, attached to the open intent,
                            // but undelivered — leave the intent OPEN;
                            // the next pass re-delivers from the stored
                            // payload instead of re-minting.
                            tracing::error!(miner = %balance.miner_id, "Token minted+persisted, delivery failed — will re-deliver from ledger on next pass: {e}");
                        }
                    }
                }
                Err(e) => {
                    // Mint never happened — restore the debit so the
                    // next pass retries the full flow.
                    if let Err(re) =
                        self.ledger
                            .rollback_payout("cashu", &balance.miner_id, balance.sats)
                    {
                        tracing::error!(miner = %balance.miner_id, "Ledger rollback failed: {re}");
                    }
                    tracing::warn!(miner = %balance.miner_id, "Cashu mint failed, will retry: {e}");
                }
            }
        }
        Ok(paid)
    }

    fn balances(&self) -> HashMap<String, u64> {
        self.ledger.balances("cashu")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_ledger() -> Arc<Ledger> {
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-cashu-{}-{}",
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

    #[test]
    fn plugin_disabled_without_probe_opt_in() {
        let res =
            CashuPlugin::from_config(tmp_ledger(), "https://mint.example.com", false, None, None);
        assert!(
            matches!(res, Err(PluginError::Other(ref m)) if m.contains("cdk")),
            "must refuse to enable without probe opt-in"
        );
    }

    #[test]
    fn malformed_destinations_env_is_a_hard_error() {
        let res = CashuPlugin::from_config(
            tmp_ledger(),
            "https://mint.example.com",
            true,
            None,
            Some("{{bad json".to_string()),
        );
        match res {
            Err(PluginError::Other(m)) => assert!(m.contains("HYDRA_CASHU_DESTINATIONS")),
            Err(other) => panic!("expected config error, got {other}"),
            Ok(_) => panic!("expected config error, got Ok"),
        }
    }

    #[test]
    fn destinations_parse_topic_and_token() {
        let map = parse_destinations(r#"{"alice":"hydrapool-alice-7f3a9b:tk_1"}"#).unwrap();
        assert_eq!(
            map.get("alice"),
            Some(&("hydrapool-alice-7f3a9b".to_string(), "tk_1".to_string()))
        );
    }

    #[test]
    fn destinations_reject_bare_public_topic() {
        // The guessable public topic form must be refused — anyone can
        // subscribe to it on ntfy.sh and read the bearer ecash.
        let err = parse_destinations(r#"{"alice":"hydrapool-alice:tk_1"}"#).unwrap_err();
        assert!(err.contains("bare public topic"), "got: {err}");
        // Separator missing entirely.
        assert!(parse_destinations(r#"{"bob":"just-a-token"}"#).is_err());
        // Empty topic or token.
        assert!(parse_destinations(r#"{"carol":":tk"}"#).is_err());
        assert!(parse_destinations(r#"{"dave":"topic:"}"#).is_err());
    }

    #[tokio::test]
    async fn deliver_requires_auth_token() {
        let mut plugin = CashuPlugin {
            ledger: tmp_ledger(),
            mint_url: "https://mint.invalid".into(),
            threshold_sats: 0,
            destinations: HashMap::new(),
            client: reqwest::Client::new(),
        };
        // No destination at all.
        let err = plugin.deliver("minerX", "token").await.unwrap_err();
        assert!(matches!(err, PluginError::NoDestination(_)));
        // Destination present but empty token — must refuse to deliver
        // bearer ecash unauthenticated.
        plugin.destinations.insert(
            "minerX".into(),
            ("hydrapool-minerX-9c1d".into(), String::new()),
        );
        let err = plugin.deliver("minerX", "token").await.unwrap_err();
        assert!(matches!(err, PluginError::Rejected(_)));
        // Empty topic — refused too.
        plugin
            .destinations
            .insert("minerX".into(), (String::new(), "tk".into()));
        let err = plugin.deliver("minerX", "token").await.unwrap_err();
        assert!(matches!(err, PluginError::Rejected(_)));
    }

    #[tokio::test]
    async fn payout_due_holds_balance_without_destination() {
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "noDest", 50_000, 1).unwrap();
        let plugin = CashuPlugin {
            ledger,
            mint_url: "https://mint.invalid".into(),
            threshold_sats: 0,
            destinations: HashMap::new(),
            client: reqwest::Client::new(),
        };
        let paid = plugin.payout_due().await.unwrap();
        assert!(paid.is_empty());
        assert_eq!(plugin.balances().get("noDest"), Some(&50_000));
    }

    #[tokio::test]
    async fn payout_due_mint_failure_restores_balance() {
        let server = wiremock::MockServer::start().await;
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "mintfail", 50_000, 1).unwrap();
        let mut destinations = HashMap::new();
        destinations.insert(
            "mintfail".to_string(),
            ("hydrapool-mintfail-1a2b".to_string(), "tok".to_string()),
        );
        let plugin = CashuPlugin {
            ledger: ledger.clone(),
            mint_url: server.uri(),
            threshold_sats: 0,
            destinations,
            client: reqwest::Client::new(),
        };
        // Quote endpoint fails.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/v1/mint/quote/bolt64"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let paid = plugin.payout_due().await.unwrap();
        assert!(paid.is_empty());
        // Balance restored — not stuck pending.
        assert_eq!(ledger.balances("cashu").get("mintfail"), Some(&50_000));
    }

    #[tokio::test]
    async fn payout_due_delivery_failure_persists_token_not_double_mint() {
        let server = wiremock::MockServer::start().await;
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "undeliv", 50_000, 1).unwrap();
        let mut destinations = HashMap::new();
        destinations.insert(
            "undeliv".to_string(),
            ("hydrapool-undeliv-4e5f".to_string(), "tok".to_string()),
        );
        let plugin = CashuPlugin {
            ledger: ledger.clone(),
            mint_url: server.uri(),
            threshold_sats: 0,
            destinations,
            client: reqwest::Client::new(),
        };
        // Mint quote + mint succeed, each exactly once. If the flow
        // re-minted after a delivery failure the .expect(1) would trip.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/v1/mint/quote/bolt64"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "quote": "q-123", "state": "UNPAID" })),
            )
            .expect(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/v1/mint/bolt64"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "signatures": [] })),
            )
            .expect(1)
            .mount(&server)
            .await;
        // Delivery goes to ntfy.sh (external, unreachable from tests)
        // and fails — the token must be persisted on a still-open
        // pending intent for re-delivery, and the intent must NOT be
        // settled (settle only happens after a successful delivery).
        let _ = plugin.payout_due().await;
        let pending = ledger.pending_payouts();
        assert_eq!(
            pending.len(),
            1,
            "delivery failure must leave the intent open"
        );
        assert_eq!(pending[0].1, "undeliv");
        assert_eq!(pending[0].2, 50_000);
        assert!(
            pending[0].3.operation_id.starts_with("minted:"),
            "minted payload must be attached to the open intent, got {:?}",
            pending[0].3.operation_id
        );
        assert!(!pending[0].3.confirmed);
        // Second payout_due pass: the first pass already debited, so
        // balances are 0 and no second quote can occur (mocks are
        // .expect(1) — a re-mint would fail the test at drop time).
        let paid2 = plugin.payout_due().await.unwrap();
        assert!(paid2.is_empty());
        drop(server); // keep mocks alive until verification completes
    }
}
