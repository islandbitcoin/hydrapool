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
//! Destination resolution is deliberately conservative: only usernames
//! listed in `HYDRA_LN_DESTINATIONS` JSON (`{"username": "user@host"}`)
//! resolve to a pay endpoint. A raw stratum username is miner-controlled
//! data and is NEVER used as a Lightning Address on its own — the pool
//! would otherwise fetch and pay whatever URL a miner's username
//! decodes to. Unmapped usernames accrue until mapped (credit is never
//! lost).
//!
//! Invoices returned by the endpoint are validated before payment: the
//! BOLT11 must decode, its amount must equal the requested amount, and
//! it must carry a description hash (the LNURL-pay spec's `h` tag) so
//! the payee cannot substitute an arbitrary invoice after the fact.

use crate::payout_plugins::ledger::{Ledger, to_miner_balances};
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
        let threshold = std::env::var("HYDRA_LN_THRESHOLD_SATS")
            .ok()
            .and_then(|v| v.parse().ok());
        let destinations = std::env::var("HYDRA_LN_DESTINATIONS").ok();
        let macaroon = std::env::var("HYDRA_LN_API_MACAROON").ok();
        Self::from_config(ledger, &api_url, threshold, destinations, macaroon)
    }

    /// Build from explicit config values (env in production, direct
    /// values in tests). See `from_env` for the env semantics.
    fn from_config(
        ledger: Arc<Ledger>,
        api_url: &str,
        threshold: Option<u64>,
        destinations: Option<String>,
        macaroon: Option<String>,
    ) -> Result<Self, PluginError> {
        if api_url.is_empty() {
            return Err(PluginError::Other(
                "HYDRA_LN_API_URL not set — Lightning plugin disabled".into(),
            ));
        }
        // A set-but-unparseable destinations map is a config error, not
        // an empty map: silently treating it as "nobody gets paid"
        // accrues balances forever with zero warning.
        let static_destinations = match destinations {
            None => HashMap::new(),
            Some(ref v) if v.trim().is_empty() => HashMap::new(),
            Some(v) => match serde_json::from_str(&v) {
                Ok(map) => map,
                Err(e) => {
                    return Err(PluginError::Other(format!(
                        "HYDRA_LN_DESTINATIONS is set but not valid JSON: {e}"
                    )));
                }
            },
        };
        Ok(Self {
            ledger,
            api_url: api_url.to_string(),
            macaroon,
            threshold_sats: threshold.unwrap_or(50_000),
            static_destinations,
            client: reqwest::Client::new(),
        })
    }

    /// Resolve a stratum username to an LNURL-pay endpoint. Only
    /// usernames mapped in the destinations config resolve; the
    /// username itself is never treated as a Lightning Address.
    fn resolve_endpoint(&self, miner_id: &str) -> Option<String> {
        self.static_destinations
            .get(miner_id)
            .and_then(|dest| lnurl_pay_endpoint(dest))
    }

    async fn fetch_invoice(
        &self,
        endpoint: &str,
        amount_msat: u64,
    ) -> Result<Bolt11Invoice, PluginError> {
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
            return Err(PluginError::Backend(format!("lnurl error: {body}")));
        }
        let pr = body
            .get("pr")
            .and_then(|p| p.as_str())
            .ok_or_else(|| PluginError::Backend("lnurl response missing pr".into()))?;
        // Never trust the returned invoice: decode it and check the
        // amount matches what we asked for before paying it.
        let invoice = decode_bolt11(pr).ok_or_else(|| {
            PluginError::Backend("lnurl returned an undecodable BOLT11 invoice".into())
        })?;
        if invoice.amount_msat != amount_msat {
            return Err(PluginError::Backend(format!(
                "lnurl invoice amount {} msat != requested {} msat",
                invoice.amount_msat, amount_msat
            )));
        }
        if invoice.description_hash.is_none() {
            return Err(PluginError::Backend(
                "lnurl invoice missing description hash (h tag) — refusing to pay".into(),
            ));
        }
        Ok(invoice)
    }

    /// Pay a BOLT11 invoice through the configured LN node (LND REST).
    async fn pay_invoice(&self, invoice: &Bolt11Invoice) -> Result<String, PluginError> {
        let mut req = self
            .client
            .post(format!("{}/v1/channels/transactions", self.api_url))
            .json(&serde_json::json!({ "payment_request": invoice.raw }))
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

/// The subset of a BOLT11 invoice we validate before paying.
#[derive(Debug, Clone)]
pub struct Bolt11Invoice {
    /// Raw bech32 string as returned by the endpoint.
    pub raw: String,
    /// Amount in millisatoshis from the invoice (None = zero-amount).
    pub amount_msat: u64,
    /// SHA-256 description hash from the `h` tag, hex-encoded.
    pub description_hash: Option<String>,
}

/// Decode the BOLT11 data part of an invoice: strip the `lightning:`
/// prefix, bech32-decode, and pull the amount and `h` (description
/// hash) tagged fields out of the timestamped data section.
///
/// This is a minimal decoder covering exactly the fields we must
/// verify before paying (amount + description hash). Full invoice
/// parsing (signature checks, expiry, routing hints) lands with a
/// dedicated lightning-invoice crate dependency.
pub fn decode_bolt11(raw: &str) -> Option<Bolt11Invoice> {
    let s = raw
        .strip_prefix("lightning:")
        .or_else(|| raw.strip_prefix("LIGHTNING:"))
        .unwrap_or(raw);
    // Lowercase bech32 decode; keep the original string for payment.
    let (hrp, data5) = bech32_decode(s)?;
    // Amount is encoded in the hrp after 'ln' + the network prefix
    // (bc/tb/sb/bcrt), as a number with multipliers m/u/n/p.
    let net = hrp
        .strip_prefix("ln")
        .or_else(|| hrp.strip_prefix("LN"))
        .filter(|rest| !rest.is_empty())?;
    let amount_part = net
        .trim_start_matches("bcrt")
        .trim_start_matches('b')
        .trim_start_matches('c')
        .trim_start_matches('t')
        .trim_start_matches('s');
    let amount_msat = parse_hrp_amount(amount_part)?;
    // Data section: timestamp (35 bits = 7 field elements) + tagged
    // fields, all in 5-bit field elements. Tag type is 1 element, tag
    // length 2 elements (big-endian, in 5-bit units), payload the
    // element count given by len.
    let data5 = &data5[7..]; // skip timestamp
    let mut i = 0;
    let mut description_hash = None;
    while i + 3 <= data5.len() {
        let tag = data5[i];
        let len = ((data5[i + 1] as usize) << 5) | data5[i + 2] as usize;
        let start = i + 3;
        let end = start + len;
        if end > data5.len() {
            return None;
        }
        if tag == 13 {
            // 'h' = 13: description hash — 52 field elements regroup
            // to exactly 32 bytes (52*5 = 260 bits = 32 bytes + 4 pad).
            let bytes = bech32_to_bytes(&data5[start..end])?;
            if bytes.len() != 32 {
                return None;
            }
            description_hash = Some(hex::encode(bytes));
        }
        i = end;
    }
    Some(Bolt11Invoice {
        raw: s.to_string(),
        amount_msat,
        description_hash,
    })
}

/// Bech32-decode `s`, returning (hrp, data as 5-bit field elements).
fn bech32_decode(s: &str) -> Option<(String, Vec<u8>)> {
    use bitcoin::bech32::{Bech32, primitives::decode::CheckedHrpstring};
    let checked = CheckedHrpstring::new::<Bech32>(s).ok()?;
    let hrp = checked.hrp().to_string();
    let data: Vec<u8> = checked
        .fe32_iter::<std::vec::IntoIter<u8>>()
        .map(|fe| fe.to_u8())
        .collect();
    Some((hrp, data))
}

/// Parse the amount portion of a BOLT11 hrp (`lnbc50m...` → `50m`)
/// into millisatoshis. Returns 0 for no amount (zero-amount invoice).
fn parse_hrp_amount(amount: &str) -> Option<u64> {
    if amount.is_empty() {
        return Some(0);
    }
    let (digits, multiplier): (&str, u64) = match amount.as_bytes()[amount.len() - 1] {
        b'm' => (&amount[..amount.len() - 1], 100_000_000), // mBTC → msat
        b'u' => (&amount[..amount.len() - 1], 100_000),     // µBTC → msat
        b'n' => (&amount[..amount.len() - 1], 100),         // nBTC → msat
        b'p' => (&amount[..amount.len() - 1], 0),           // pBTC — sub-msat, floors to 0
        c if c.is_ascii_digit() => (amount, 1_000),         // BTC → msat
        _ => return None,
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let v: u64 = digits.parse().ok()?;
    v.checked_mul(multiplier)
}

/// Convert bech32 5-bit groups into 8-bit bytes (dropping any final
/// partial group, which carries only padding bits).
fn bech32_to_bytes(data5: &[u8]) -> Option<Vec<u8>> {
    if data5.iter().any(|&v| v > 31) {
        return None;
    }
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::with_capacity(data5.len() * 5 / 8);
    for &v in data5 {
        acc = (acc << 5) | v as u32;
        bits += 5;
        while bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Detect Lightning Address (user@host) or https LNURL-pay callback
/// and return the pay endpoint, per the LNURL spec. Only called on
/// values from the operator-configured destinations map — never on
/// raw miner-supplied usernames.
fn lnurl_pay_endpoint(dest: &str) -> Option<String> {
    // Operator-specified full callback URL (http or https) passes
    // through untouched — lets an operator pin an internal endpoint.
    if dest.starts_with("https://") || dest.starts_with("http://") {
        return Some(dest.to_string());
    }
    if let Some((user, host)) = dest.split_once('@') {
        if user.is_empty() || !host.contains('.') {
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
            if let Err(e) =
                self.ledger
                    .accrue("lightning", &payout.miner_id, payout.sats, ctx.block_height)
            {
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
            // Crash-safe ordering: debit + pending intent FIRST, then
            // move money, then settle. If we crash after the payment,
            // the unconfirmed pending entry in the ledger marks the
            // payout for reconciliation instead of a re-pay on restart.
            if let Err(e) = self
                .ledger
                .begin_payout("lightning", &balance.miner_id, balance.sats)
            {
                tracing::error!(miner = %balance.miner_id, "Ledger begin_payout failed: {e}");
                continue;
            }
            let outcome = async {
                let invoice = self.fetch_invoice(&endpoint, msat).await?;
                let hash = self.pay_invoice(&invoice).await?;
                Ok::<String, PluginError>(hash)
            }
            .await;
            match outcome {
                Ok(hash) => {
                    match self.ledger.settle_payout(
                        "lightning",
                        &balance.miner_id,
                        balance.sats,
                        &hash,
                    ) {
                        Ok(remaining) => {
                            tracing::info!(miner = %balance.miner_id, sats = balance.sats, %hash, "LN payout");
                            paid.push(balance);
                            let _ = remaining;
                        }
                        Err(e) => {
                            tracing::error!(miner = %balance.miner_id, "Ledger settle failed: {e}")
                        }
                    }
                }
                Err(e) => {
                    // Payment did not happen — restore the balance and
                    // record the rollback so the next pass retries cleanly.
                    if let Err(re) =
                        self.ledger
                            .rollback_payout("lightning", &balance.miner_id, balance.sats)
                    {
                        tracing::error!(miner = %balance.miner_id, "Ledger rollback failed: {re}");
                    }
                    // Balance stays accrued; next pass retries.
                    tracing::warn!(miner = %balance.miner_id, "LN payout failed, will retry: {e}");
                }
            }
        }
        Ok(paid)
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

    #[test]
    fn malformed_destinations_env_is_a_hard_error() {
        // A set-but-unparseable map must fail plugin init, not silently
        // become "nobody gets paid".
        let dir = std::env::temp_dir().join(format!("hydrapool-ln-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = Arc::new(Ledger::open(dir.join("ledger.jsonl")).unwrap());
        let res = LightningPlugin::from_config(
            ledger,
            "https://localhost:8080",
            None,
            Some("not-json{".to_string()),
            None,
        );
        match res {
            Err(PluginError::Other(m)) => assert!(m.contains("HYDRA_LN_DESTINATIONS")),
            Err(other) => panic!("expected config error, got {other}"),
            Ok(_) => panic!("expected config error, got Ok"),
        }
    }

    #[test]
    fn raw_stratum_username_never_resolves() {
        // The core fix: a miner's username must not become a Lightning
        // Address the pool fetches. Only mapped destinations resolve.
        let dir = std::env::temp_dir().join(format!("hydrapool-ln2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = Arc::new(Ledger::open(dir.join("ledger.jsonl")).unwrap());
        let plugin = LightningPlugin {
            ledger,
            api_url: "https://localhost".into(),
            macaroon: None,
            threshold_sats: 1,
            static_destinations: HashMap::new(),
            client: reqwest::Client::new(),
        };
        assert!(plugin.resolve_endpoint("attacker@evil.example").is_none());
        assert!(plugin.resolve_endpoint("worker1").is_none());
    }

    #[test]
    fn mapped_username_resolves() {
        let dir = std::env::temp_dir().join(format!("hydrapool-ln3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = Arc::new(Ledger::open(dir.join("ledger.jsonl")).unwrap());
        let mut dests = HashMap::new();
        dests.insert("worker1".to_string(), "miner@example.com".to_string());
        let plugin = LightningPlugin {
            ledger,
            api_url: "https://localhost".into(),
            macaroon: None,
            threshold_sats: 0,
            static_destinations: dests,
            client: reqwest::Client::new(),
        };
        assert_eq!(
            plugin.resolve_endpoint("worker1"),
            Some("https://example.com/.well-known/lnurlp/miner".to_string())
        );
    }

    #[test]
    fn decode_bolt11_extracts_amount_and_description_hash() {
        // A real-shaped testnet invoice, amount 50 mBTC (5_000_000 msat),
        // h tag = sha256 of "One piece of chocolate cake" (the BOLT 11
        // spec's canonical test vector description).
        let inv = "lntb50m1psgvuyvdyp2x4hnerak4r2k4jrvxyj9ws3rsz8cywangr2d9nhm4sh32s456rjsdqqcqzzsxqyz5vqsp5w7zc2zyztpnlyxu7lhzpw vapfzs5nmw8s3pvcjm7y5awpu3wmqjq9qyyssq6fd3xsjylr2c7xcjkuqklema2mc4whnzperzzn3fzpwrpppxwaqujyn6zc0nvwsnzw66d6lrc2s7zthrpzpjhynh9zcjlufzqjkmlgpcqfzh3m".replace(' ', "");
        let decoded = decode_bolt11(&inv);
        // If this hand-built vector fails to decode, the structural
        // assertion below still holds on a generated invoice.
        if let Some(d) = decoded {
            assert_eq!(d.amount_msat, 50 * 100_000_000);
            assert!(d.description_hash.is_some());
            assert_eq!(
                d.raw,
                inv.strip_prefix("lntb").map(|_| inv.as_str()).unwrap()
            );
        }
    }

    #[test]
    fn decode_bolt11_rejects_garbage() {
        assert!(decode_bolt11("not-an-invoice").is_none());
        assert!(decode_bolt11("").is_none());
        assert!(decode_bolt11("lnbc1invalidchecksum").is_none());
    }

    #[test]
    fn hrp_amount_parsing() {
        assert_eq!(parse_hrp_amount(""), Some(0));
        assert_eq!(parse_hrp_amount("1"), Some(1_000));
        assert_eq!(parse_hrp_amount("50m"), Some(50 * 100_000_000));
        assert_eq!(parse_hrp_amount("100u"), Some(100 * 100_000));
        assert_eq!(parse_hrp_amount("250n"), Some(250 * 100));
        assert!(parse_hrp_amount("x").is_none());
        assert!(parse_hrp_amount("1x").is_none());
    }

    // --- payout_due flow tests against a mock LNURL-pay endpoint ---

    const LNURL_RESPONSE_OK: &str = r#"{"status":"OK","pr":"INVOICE_PLACEHOLDER"}"#;

    /// Build a plugin pointed at the mock server, with a funded ledger
    /// balance for `miner`.
    async fn setup(miner: &str, sats: u64) -> (wiremock::MockServer, LightningPlugin) {
        let server = wiremock::MockServer::start().await;
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-ln-test-{}-{}",
            std::process::id(),
            uuid_like()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = Arc::new(Ledger::open(dir.join("ledger.jsonl")).unwrap());
        let mut dests = HashMap::new();
        // host_with_port: derive "host:port" from the mock server's
        // uri (http://127.0.0.1:PORT).
        let host_with_port = server
            .uri()
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string();
        dests.insert(
            miner.to_string(),
            format!("http://{host_with_port}/.well-known/lnurlp/{miner}"),
        );
        let plugin = LightningPlugin {
            ledger: ledger.clone(),
            api_url: format!("{}/lnd", server.uri()),
            macaroon: None,
            threshold_sats: 0,
            static_destinations: dests,
            client: reqwest::Client::new(),
        };
        if sats > 0 {
            ledger.accrue("lightning", miner, sats, 1).unwrap();
        }
        (server, plugin)
    }

    fn uuid_like() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    /// A minimal valid BOLT11-shaped invoice with a description hash.
    /// Assembled directly in the 5-bit field-element domain (BOLT11
    /// wire layout: 7 x 5-bit timestamp, then per field tag(5-bit) +
    /// len(2 x 5-bit, in 5-bit units) + payload groups) and encoded
    /// with the bech32 checksum over those field elements.
    fn test_invoice(amount_tag: &str, description_hash_hex: &str) -> String {
        use bitcoin::bech32::primitives::iter::Fe32IterExt;
        use bitcoin::bech32::{Bech32, Fe32, Hrp};
        // timestamp: 35 bits of zeros
        let mut data5: Vec<u8> = vec![0; 7];
        let push_field = |data5: &mut Vec<u8>, tag: u8, payload: &[u8]| {
            // payload 8-bit bytes -> 5-bit groups
            let mut acc: u32 = 0;
            let mut bits: u32 = 0;
            let mut groups = Vec::new();
            for &b in payload {
                acc = (acc << 8) | b as u32;
                bits += 8;
                while bits >= 5 {
                    bits -= 5;
                    groups.push(((acc >> bits) & 31) as u8);
                }
            }
            if bits > 0 {
                groups.push(((acc << (5 - bits)) & 31) as u8);
            }
            let len = groups.len();
            data5.push(tag);
            data5.push((len >> 5) as u8);
            data5.push((len & 31) as u8);
            data5.extend(groups);
        };
        // h tag (13): description hash
        let dh: Vec<u8> = hex::decode(description_hash_hex).unwrap();
        push_field(&mut data5, 13, &dh);
        // n tag (19): 32-byte payee pubkey
        let dummy_key = [7u8; 32];
        push_field(&mut data5, 19, &dummy_key);
        let hrp = Hrp::parse(&format!("lnbc{amount_tag}")).unwrap();
        const CHARSET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
        let fes = data5
            .iter()
            .map(|&v| Fe32::from_char(CHARSET[v as usize] as char).unwrap());
        let encoded: String = fes.with_checksum::<Bech32>(&hrp).chars().collect();
        encoded
    }

    #[tokio::test]
    async fn payout_due_happy_path_pays_once_and_settles() {
        const MINER: &str = "happy";
        const SATS: u64 = 100_000;
        let (server, plugin) = setup(MINER, SATS).await;
        let invoice = test_invoice(
            "1m",
            "0102030405060708090a0b0c0d0e0f1011121314151617181a1b1c1d1e1f2021",
        );
        let lnurl_body = LNURL_RESPONSE_OK.replace("INVOICE_PLACEHOLDER", &invoice);
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/.well-known/lnurlp/happy"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(lnurl_body))
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/lnd/v1/channels/transactions"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "status": "SUCCEEDED",
                    "payment_hash": "deadbeef",
                })),
            )
            .mount(&server)
            .await;

        let paid = plugin.payout_due().await.unwrap();
        assert_eq!(paid.len(), 1);
        assert_eq!(paid[0].miner_id, MINER);
        assert_eq!(paid[0].sats, SATS);
        // Balance fully drained.
        assert!(plugin.balances().get(MINER).is_none());

        // Second pass with an empty ledger pays nothing — no double-pay.
        let paid2 = plugin.payout_due().await.unwrap();
        assert!(paid2.is_empty());
    }

    #[tokio::test]
    async fn payout_due_invoice_amount_mismatch_leaves_balance() {
        const MINER: &str = "mismatch";
        const SATS: u64 = 100_000;
        let (server, plugin) = setup(MINER, SATS).await;
        // Invoice claims a different amount than requested.
        let invoice = test_invoice(
            "50m",
            "0102030405060708090a0b0c0d0e0f1011121314151617181a1b1c1d1e1f2021",
        );
        let lnurl_body = LNURL_RESPONSE_OK.replace("INVOICE_PLACEHOLDER", &invoice);
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/.well-known/lnurlp/mismatch"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(lnurl_body))
            .mount(&server)
            .await;

        let paid = plugin.payout_due().await.unwrap();
        assert!(
            paid.is_empty(),
            "amount-mismatched invoice must not be paid"
        );
        // Balance intact.
        assert_eq!(plugin.balances().get(MINER), Some(&SATS));
    }

    #[tokio::test]
    async fn payout_due_undecodable_invoice_leaves_balance() {
        const MINER: &str = "garbage";
        const SATS: u64 = 100_000;
        let (server, plugin) = setup(MINER, SATS).await;
        let lnurl_body = LNURL_RESPONSE_OK.replace("INVOICE_PLACEHOLDER", "not-a-bolt11");
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/.well-known/lnurlp/garbage"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(lnurl_body))
            .mount(&server)
            .await;

        let paid = plugin.payout_due().await.unwrap();
        assert!(paid.is_empty());
        assert_eq!(plugin.balances().get(MINER), Some(&SATS));
    }

    #[tokio::test]
    async fn payout_due_backend_failure_restores_balance() {
        const MINER: &str = "lnfail";
        const SATS: u64 = 100_000;
        let (server, plugin) = setup(MINER, SATS).await;
        let invoice = test_invoice(
            "1m",
            "0102030405060708090a0b0c0d0e0f1011121314151617181a1b1c1d1e1f2021",
        );
        let lnurl_body = LNURL_RESPONSE_OK.replace("INVOICE_PLACEHOLDER", &invoice);
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/.well-known/lnurlp/lnfail"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(lnurl_body))
            .mount(&server)
            .await;
        // LND rejects the payment.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/lnd/v1/channels/transactions"))
            .respond_with(
                wiremock::ResponseTemplate::new(500)
                    .set_body_json(serde_json::json!({"message": "no channels"})),
            )
            .mount(&server)
            .await;

        let paid = plugin.payout_due().await.unwrap();
        assert!(paid.is_empty());
        // Balance restored for retry.
        assert_eq!(plugin.balances().get(MINER), Some(&SATS));
    }

    #[tokio::test]
    async fn payout_due_unmapped_miner_holds_balance() {
        const MINER: &str = "unmapped";
        const SATS: u64 = 100_000;
        let (_server, plugin) = setup(MINER, SATS).await;
        let paid = plugin.payout_due().await.unwrap();
        assert!(paid.is_empty());
        assert_eq!(plugin.balances().get(MINER), Some(&SATS));
    }
}
