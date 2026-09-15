//! Persistent per-miner accrual ledger shared by plugins.
//!
//! Backed by an append-only JSON-lines file next to the pool store so
//! balances survive restarts without adding a database dependency.
//! Each line is one mutation; state is replayed on load.

use crate::payout_plugins::traits::MinerBalance;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    /// Plugin that owns this balance ("lightning", "cashu", "fedimint").
    /// Kept for compatibility with existing ledger files; `rail` is the
    /// forward-looking alias written on all new entries and is
    /// currently identical to the plugin name.
    plugin: String,
    /// Rail namespace for this entry — same value as `plugin` today
    /// ("lightning", "cashu", "fedimint"). Replay accepts entries with
    /// either or both fields; `rail` wins when both are present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rail: Option<String>,
    miner_id: String,
    /// Delta in sats: positive on accrual, negative on payout.
    delta_sats: i64,
    /// Unix seconds.
    ts: u64,
    /// Height of the block that triggered the entry (0 for payouts).
    block_height: u32,
    /// Optional pending-payout marker. When present, this entry moved
    /// `delta_sats` out of the accrued balance but the payment has not
    /// been confirmed yet; `confirmed == false` entries must be
    /// reconciled against the backend on restart before re-paying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending: Option<PendingPayout>,
}

impl LedgerEntry {
    /// The rail this entry belongs to: `rail` when present, else
    /// `plugin` (v1 entries have only `plugin`).
    fn rail_or_plugin(&self) -> &str {
        self.rail.as_deref().unwrap_or(&self.plugin)
    }
}

/// A payout dispatched but not yet settled. Written to the ledger
/// BEFORE the payment is sent so a crash between pay and record leaves
/// a recoverable trace instead of double-paying on the next tick.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingPayout {
    /// Backend identifier for the in-flight payment (LND payment_hash,
    /// gateway operation_id, mint quote id). Empty when the backend
    /// has not yet assigned one.
    pub operation_id: String,
    /// Backend receipt id captured at dispatch time when known
    /// (payment hash / mint quote id / gateway operation id). Unlike
    /// `operation_id` — which is filled in as the flow progresses and
    /// is reused for `minted:` payloads — `idem_key` is stamped at
    /// settle time as the canonical backend receipt for auditing and
    /// manual backend reconciliation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idem_key: Option<String>,
    /// true once the backend confirmed the payment settled.
    pub confirmed: bool,
}

/// In-memory ledger state: plugin -> miner_id -> accrued sats.
// Under non-default feature combos (e.g. no rails at all) most of the
// ledger API has no in-tree caller — dead_code must not fire there.
#[cfg_attr(
    not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
    allow(dead_code)
)]
#[derive(Default)]
pub struct Ledger {
    path: PathBuf,
    state: Mutex<HashMap<(String, String), i64>>,
}

impl Ledger {
    /// Open (or create) the ledger at `path`, replaying any existing
    /// entries. Corrupt trailing lines (torn writes) are skipped.
    // Most of this impl is called only by rail plugins; the
    // all-rails-disabled build flags those methods dead.
    #[cfg_attr(
        not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
        allow(dead_code)
    )]
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let mut state: HashMap<(String, String), i64> = HashMap::new();
        if path.exists() {
            let file = std::fs::File::open(&path)?;
            for line in std::io::BufReader::new(file).lines() {
                let line = line?;
                let Ok(entry) = serde_json::from_str::<LedgerEntry>(&line) else {
                    // Torn final write or junk line — replay stops cleanly
                    // only if it's the last line; otherwise log and skip.
                    tracing::warn!("Skipping unparseable ledger line: {line}");
                    continue;
                };
                // v1 entries carry only `plugin`; new entries also carry
                // `rail` (same value today). `rail` wins when both exist.
                let rail = entry.rail_or_plugin().to_string();
                *state.entry((rail, entry.miner_id)).or_insert(0) += entry.delta_sats;
            }
        } else if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    /// Accrue `sats` for `miner_id` under `plugin` for block `height`.
    // Called by rail plugins' on_block; the all-rails-disabled build
    // would otherwise flag it dead.
    #[cfg_attr(
        not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
        allow(dead_code)
    )]
    pub fn accrue(
        &self,
        plugin: &str,
        miner_id: &str,
        sats: u64,
        block_height: u32,
    ) -> std::io::Result<()> {
        self.append(Self::entry(
            plugin,
            miner_id,
            sats as i64,
            block_height,
            None,
        ))
    }

    /// Build a ledger entry. `rail` is always written (forward-looking
    /// alias, identical to `plugin` today); `pending` carries the
    /// pending-intent marker when one applies.
    fn entry(
        plugin: &str,
        miner_id: &str,
        delta_sats: i64,
        _block_height: u32,
        pending: Option<PendingPayout>,
    ) -> LedgerEntry {
        LedgerEntry {
            plugin: plugin.to_string(),
            rail: Some(plugin.to_string()),
            miner_id: miner_id.to_string(),
            delta_sats,
            ts: now(),
            block_height: _block_height,
            pending,
        }
    }

    /// Peek balances for one plugin without mutating.
    // Called by rail plugins' payout_due; the all-rails-disabled build
    // would otherwise flag it dead.
    #[cfg_attr(
        not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
        allow(dead_code)
    )]
    pub fn balances(&self, plugin: &str) -> HashMap<String, u64> {
        self.state
            .lock()
            .unwrap()
            .iter()
            .filter(|((p, _), v)| p == plugin && **v > 0)
            .map(|((_, m), v)| (m.clone(), *v as u64))
            .collect()
    }

    /// Open a pending payout: atomically debit `sats` from the accrued
    /// balance and write an unconfirmed pending entry. Call BEFORE
    /// dispatching the payment. Returns the remaining balance, or an
    /// error if the balance is insufficient. On success the miner's
    /// accrued balance is reduced; if the payment later fails, call
    /// [`Ledger::rollback_payout`] to restore it.
    // Called by rail plugins' payout_due; the all-rails-disabled build
    // would otherwise flag it dead.
    #[cfg_attr(
        not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
        allow(dead_code)
    )]
    pub fn begin_payout(&self, plugin: &str, miner_id: &str, sats: u64) -> Result<u64, String> {
        let entry = Self::entry(
            plugin,
            miner_id,
            -(sats as i64),
            0,
            Some(PendingPayout {
                operation_id: String::new(),
                idem_key: None,
                confirmed: false,
            }),
        );
        // Look up with borrowed keys — only allocate when the entry is
        // genuinely new (append_entry's entry() call).
        let new_balance = {
            let mut state = self.state.lock().unwrap();
            let balance = borrowed_balance(&state, plugin, miner_id);
            if (sats as i64) > balance {
                return Err(format!(
                    "payout {sats} sats exceeds accrued {balance} sats for {miner_id}"
                ));
            }
            let new_balance = balance - sats as i64;
            self.append_entry(&mut state, entry)
                .map_err(|e| e.to_string())?;
            new_balance
        };
        Ok(new_balance as u64)
    }

    /// Confirm a pending payout after the backend reported success.
    /// `sats` is the amount that was paid (for log/audit parity with
    /// `begin_payout`); the accrued balance was already debited by
    /// `begin_payout`, so this only records the confirmation.
    /// `idem_key` is the backend receipt id (payment hash / quote id /
    /// operation id) when known — stamped onto the settle entry so
    /// operators can reconcile against the backend by that key.
    /// Returns the remaining accrued balance.
    // Called by rail plugins' payout_due; the all-rails-disabled build
    // would otherwise flag it dead.
    #[cfg_attr(
        not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
        allow(dead_code)
    )]
    pub fn settle_payout(
        &self,
        plugin: &str,
        miner_id: &str,
        _sats: u64,
        operation_id: &str,
    ) -> Result<u64, String> {
        self.settle_payout_with_idem(plugin, miner_id, operation_id, operation_id)
    }

    /// Like [`Ledger::settle_payout`] but with an explicit idempotency
    /// key distinct from `operation_id` (e.g. settle by an attached
    /// `minted:` payload while stamping the mint quote id as the
    /// backend receipt).
    // Called by the cashu rail; other single-rail builds would
    // otherwise flag it dead.
    #[cfg_attr(not(feature = "cashu"), allow(dead_code))]
    pub fn settle_payout_with_idem(
        &self,
        plugin: &str,
        miner_id: &str,
        operation_id: &str,
        idem_key: &str,
    ) -> Result<u64, String> {
        let entry = Self::entry(
            plugin,
            miner_id,
            0,
            0,
            Some(PendingPayout {
                operation_id: operation_id.to_string(),
                idem_key: Some(idem_key.to_string()),
                confirmed: true,
            }),
        );
        let balance = {
            let mut state = self.state.lock().unwrap();
            self.append_entry(&mut state, entry)
                .map_err(|e| e.to_string())?;
            borrowed_balance(&state, plugin, miner_id)
        };
        Ok(balance as u64)
    }

    /// Roll a pending payout back (payment did not happen): restore the
    /// accrued balance and leave a confirmed zero-delta note explaining
    /// why. Idempotent per call site — callers invoke this exactly once
    /// per failed payment.
    pub fn rollback_payout(&self, plugin: &str, miner_id: &str, sats: u64) -> Result<u64, String> {
        let entry = Self::entry(
            plugin,
            miner_id,
            sats as i64,
            0,
            Some(PendingPayout {
                operation_id: String::new(),
                idem_key: None,
                confirmed: true,
            }),
        );
        let balance = {
            let mut state = self.state.lock().unwrap();
            self.append_entry(&mut state, entry)
                .map_err(|e| e.to_string())?;
            borrowed_balance(&state, plugin, miner_id)
        };
        Ok(balance as u64)
    }

    /// Read the still-open pending payout entries from the ledger
    /// file. Used at startup to reconcile in-flight payments against
    /// the backend instead of re-paying them, and by the cashu plugin
    /// to find minted-but-undelivered tokens for re-delivery.
    ///
    /// A `begin_payout` opens a pending debit; a later `settle_payout`
    /// (zero-delta, confirmed) or `rollback_payout` (positive delta,
    /// confirmed) closes the oldest open pending debit for the same
    /// (plugin, miner). A zero-delta UNCONFIRMED entry with a non-empty
    /// operation id ([`Ledger::attach_pending_payload`]) records a
    /// payload (e.g. a minted ecash token) on the most recent open
    /// intent. Entries left open after pairing are returned.
    pub fn pending_payouts(&self) -> Vec<(String, String, u64, PendingPayout)> {
        let mut open: Vec<(String, String, u64, PendingPayout)> = Vec::new();
        if !self.path.exists() {
            return open;
        }
        if let Ok(file) = std::fs::File::open(&self.path) {
            for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
                let Ok(entry) = serde_json::from_str::<LedgerEntry>(&line) else {
                    continue;
                };
                let Some(ref p) = entry.pending else { continue };
                // v1 entries carry only `plugin`; rail wins when both
                // fields exist.
                let rail = entry.rail_or_plugin().to_string();
                if !p.confirmed && entry.delta_sats < 0 {
                    open.push((rail, entry.miner_id, (-entry.delta_sats) as u64, p.clone()));
                } else if !p.confirmed && entry.delta_sats == 0 && !p.operation_id.is_empty() {
                    // Payload attach: record it on the most recent open
                    // intent for this (rail, miner).
                    if let Some(slot) = open
                        .iter_mut()
                        .rev()
                        .find(|(pl, m, _, _)| *pl == rail && *m == entry.miner_id)
                    {
                        slot.3.operation_id = p.operation_id.clone();
                    }
                } else if p.confirmed {
                    // A settle (delta 0) or rollback (delta > 0) closes
                    // the oldest open pending debit for this miner.
                    if let Some(pos) = open
                        .iter()
                        .position(|(pl, m, _, _)| *pl == rail && *m == entry.miner_id)
                    {
                        open.remove(pos);
                    }
                }
            }
        }
        open
    }

    /// Attach a payload (e.g. a minted ecash token) to the still-open
    /// pending payout for (plugin, miner) as a zero-delta unconfirmed
    /// entry. Written BEFORE delivery so a delivery failure or crash
    /// leaves the payload recoverable from the ledger — re-deliver it,
    /// never re-mint. No-op on balances; must be called between
    /// `begin_payout` and `settle_payout`/`rollback_payout`.
    // Only the cashu rail calls this; other single-rail builds would
    // otherwise flag it dead.
    #[cfg_attr(not(feature = "cashu"), allow(dead_code))]
    pub fn attach_pending_payload(
        &self,
        plugin: &str,
        miner_id: &str,
        payload: &str,
    ) -> std::io::Result<()> {
        self.append(Self::entry(
            plugin,
            miner_id,
            0,
            0,
            Some(PendingPayout {
                operation_id: payload.to_string(),
                idem_key: None,
                confirmed: false,
            }),
        ))
    }

    /// Startup reconciliation for stranded pending payouts.
    ///
    /// Returns every still-open pending intent and logs an error per
    /// intent with plugin/miner/sats/operation_id, so a crash between
    /// `begin_payout` and settle/rollback is visible to the operator
    /// instead of silently stranding the miner's debited credit.
    /// Backend-specific settlement (query LND by payment_hash, the
    /// gateway by operation_id, ...) is a manual follow-up until a
    /// payment-status API lands per plugin; the cashu plugin
    /// additionally re-delivers `minted:` payloads automatically.
    pub fn stranded_pending_payouts(&self) -> Vec<(String, String, u64, PendingPayout)> {
        let open = self.pending_payouts();
        for (plugin, miner, sats, pending) in &open {
            // An intent that already carries an idem_key can be
            // reconciled directly against the backend by that receipt
            // id — surface it prominently for the operator.
            if let Some(idem) = &pending.idem_key {
                tracing::error!(
                    plugin = %plugin,
                    miner = %miner,
                    sats = sats,
                    operation_id = %pending.operation_id,
                    idem_key = %idem,
                    "STRANDED PAYOUT from a previous run: balance debited, payment state unknown — reconcile against the backend by idem_key/operation_id"
                );
            } else {
                tracing::error!(
                    plugin = %plugin,
                    miner = %miner,
                    sats = sats,
                    operation_id = %pending.operation_id,
                    "STRANDED PAYOUT from a previous run: balance debited, payment state unknown — reconcile against the backend by operation_id"
                );
            }
        }
        open
    }

    fn append(&self, entry: LedgerEntry) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        self.append_entry(&mut state, entry)
    }

    /// Append a serialized entry to disk and update in-memory state.
    /// Caller must already hold the state lock.
    fn append_entry(
        &self,
        state: &mut HashMap<(String, String), i64>,
        entry: LedgerEntry,
    ) -> std::io::Result<()> {
        // Serialize first, then append under one fs op, so a torn write
        // can only ever drop the trailing line, which replay tolerates.
        let line = serde_json::to_string(&entry).expect("ledger entry serializes");
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(f, "{line}")?;
        f.flush()?;
        *state.entry((entry.plugin, entry.miner_id)).or_insert(0) += entry.delta_sats;
        Ok(())
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Current balance for (plugin, miner) without allocating the key.
/// `HashMap::get` on a `(String, String)` key requires an owned pair,
/// so a linear scan is the allocation-free option — at pool scale
/// (~100 miners) it beats two String builds per call.
// Free functions in this module are called only by rail plugins; the
// all-rails-disabled build flags them dead.
#[cfg_attr(
    not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
    allow(dead_code)
)]
fn borrowed_balance(state: &HashMap<(String, String), i64>, plugin: &str, miner_id: &str) -> i64 {
    state
        .iter()
        .find(|((p, m), _)| p == plugin && m == miner_id)
        .map_or(0, |(_, v)| *v)
}

/// Convert raw balances into [`MinerBalance`] list, dropping zero entries.
// Free functions in this module are called only by rail plugins; the
// all-rails-disabled build flags them dead.
#[cfg_attr(
    not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
    allow(dead_code)
)]
pub fn to_miner_balances(balances: HashMap<String, u64>) -> Vec<MinerBalance> {
    balances
        .into_iter()
        .filter(|(_, sats)| *sats > 0)
        .map(|(miner_id, sats)| MinerBalance { miner_id, sats })
        .collect()
}

/// Uniform payout-failure handling for every plugin (see the
/// error-semantics note on [`crate::payout_plugins::traits::PayoutPlugin`]):
/// an [`crate::payout_plugins::traits::PluginError::Ambiguous`] outcome
/// may still settle, so the pending intent stays OPEN for
/// reconciliation; any definite pre-dispatch failure rolls the intent
/// back, restoring the balance so the next pass retries cleanly.
// Free functions in this module are called only by rail plugins; the
// all-rails-disabled build flags them dead.
#[cfg_attr(
    not(any(feature = "ln", feature = "cashu", feature = "fedimint")),
    allow(dead_code)
)]
pub fn rollback_on_definite_failure(
    ledger: &Ledger,
    plugin: &str,
    balance: &MinerBalance,
    err: &crate::payout_plugins::traits::PluginError,
) {
    use crate::payout_plugins::traits::PluginError;
    if matches!(err, PluginError::Ambiguous(_)) {
        tracing::warn!(
            plugin,
            miner = %balance.miner_id,
            "Payout outcome unknown ({err}) — pending intent left OPEN for reconciliation"
        );
        return;
    }
    match ledger.rollback_payout(plugin, &balance.miner_id, balance.sats) {
        Ok(_) => tracing::warn!(
            plugin,
            miner = %balance.miner_id,
            "Payout failed pre-dispatch ({err}) — balance restored, will retry"
        ),
        Err(re) => tracing::error!(
            plugin,
            miner = %balance.miner_id,
            "Ledger rollback failed: {re}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique-per-call suffix for test temp dirs: tests run in
    /// parallel in one process, so pid alone is not unique enough to
    /// keep one test's remove_dir_all from deleting another's files.
    /// An atomic counter guarantees uniqueness within the process
    /// (clock nanos can collide on platforms with ~1us resolution).
    fn test_dir_nonce() -> u128 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst) as u128;
        (n << 64)
            | (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                & 0xffff_ffff_ffff_ffff)
    }

    fn tmp_ledger() -> Ledger {
        // Key by pid + unique nonce: tests run in parallel within one
        // process, so a pid-only name would let one test's
        // remove_dir_all yank the files out from under another
        // concurrently running test.
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-ledger-{}-{}",
            std::process::id(),
            test_dir_nonce()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Ledger::open(dir.join("ledger.jsonl")).unwrap()
    }

    #[test]
    fn accrue_and_payout_roundtrip() {
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "npub123", 5_000, 800).unwrap();
        ledger.accrue("cashu", "npub123", 2_500, 801).unwrap();
        assert_eq!(ledger.balances("cashu").get("npub123"), Some(&7_500));
        let remaining = ledger
            .begin_payout("cashu", "npub123", 7_500)
            .and_then(|_| ledger.settle_payout("cashu", "npub123", 7_500, "op"))
            .unwrap();
        assert_eq!(remaining, 0);
        assert!(ledger.balances("cashu").get("npub123").is_none());
    }

    #[test]
    fn payout_over_balance_rejected() {
        let ledger = tmp_ledger();
        ledger.accrue("lightning", "worker1", 100, 900).unwrap();
        assert!(ledger.begin_payout("lightning", "worker1", 101).is_err());
        // Rejected payout must not have mutated state.
        assert_eq!(ledger.balances("lightning").get("worker1"), Some(&100));
    }

    #[test]
    fn plugins_are_isolated() {
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "minerA", 500, 1).unwrap();
        ledger.accrue("fedimint", "minerA", 700, 1).unwrap();
        assert_eq!(ledger.balances("cashu").get("minerA"), Some(&500));
        assert_eq!(ledger.balances("fedimint").get("minerA"), Some(&700));
    }

    #[test]
    fn state_replays_from_disk() {
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-replay-{}-{}",
            std::process::id(),
            test_dir_nonce()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        {
            let ledger = Ledger::open(path.clone()).unwrap();
            ledger.accrue("cashu", "npubX", 1_000, 10).unwrap();
            ledger.begin_payout("cashu", "npubX", 400).unwrap();
            ledger.settle_payout("cashu", "npubX", 400, "op").unwrap();
        }
        let reopened = Ledger::open(path).unwrap();
        assert_eq!(reopened.balances("cashu").get("npubX"), Some(&600));
    }

    #[test]
    fn torn_final_line_is_skipped() {
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-torn-{}-{}",
            std::process::id(),
            test_dir_nonce()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        let ledger = Ledger::open(path.clone()).unwrap();
        ledger.accrue("cashu", "npubY", 900, 3).unwrap();
        drop(ledger);
        // Simulate a torn write: append a partial JSON line.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            use std::io::Write;
            write!(f, "{{\"plugin\":\"ca").unwrap();
        }
        let reopened = Ledger::open(path).unwrap();
        assert_eq!(reopened.balances("cashu").get("npubY"), Some(&900));
    }

    #[test]
    fn pending_payout_debits_then_rollback_restores() {
        let ledger = tmp_ledger();
        ledger.accrue("lightning", "minerP", 1_000, 5).unwrap();
        // begin debits the balance immediately.
        ledger.begin_payout("lightning", "minerP", 600).unwrap();
        assert_eq!(ledger.balances("lightning").get("minerP"), Some(&400));
        // Payment failed — restore.
        ledger.rollback_payout("lightning", "minerP", 600).unwrap();
        assert_eq!(ledger.balances("lightning").get("minerP"), Some(&1_000));
    }

    #[test]
    fn pending_payout_settle_keeps_debit() {
        let ledger = tmp_ledger();
        ledger.accrue("lightning", "minerS", 1_000, 5).unwrap();
        ledger.begin_payout("lightning", "minerS", 600).unwrap();
        ledger
            .settle_payout("lightning", "minerS", 600, "abc123")
            .unwrap();
        // Payment succeeded — debit stays.
        assert_eq!(ledger.balances("lightning").get("minerS"), Some(&400));
    }

    #[test]
    fn begin_payout_over_balance_rejected() {
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "minerB", 100, 1).unwrap();
        assert!(ledger.begin_payout("cashu", "minerB", 101).is_err());
        assert_eq!(ledger.balances("cashu").get("minerB"), Some(&100));
    }

    #[test]
    fn unconfirmed_payouts_replay_after_restart() {
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-pending-{}-{}",
            std::process::id(),
            test_dir_nonce()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        {
            let ledger = Ledger::open(path.clone()).unwrap();
            ledger.accrue("lightning", "minerR", 2_000, 1).unwrap();
            ledger.begin_payout("lightning", "minerR", 1_500).unwrap();
            ledger
                .settle_payout("lightning", "minerR", 1_500, "op1")
                .unwrap();
            ledger.begin_payout("lightning", "minerR", 300).unwrap();
        }
        let reopened = Ledger::open(path).unwrap();
        let pending = reopened.pending_payouts();
        assert_eq!(pending.len(), 1, "only the unsettled begin survives");
        let (plugin, miner, sats, p) = &pending[0];
        assert_eq!(plugin, "lightning");
        assert_eq!(miner, "minerR");
        assert_eq!(*sats, 300);
        assert!(!p.confirmed);
        // The confirmed settle must not appear, and balance reflects
        // both debits.
        assert_eq!(reopened.balances("lightning").get("minerR"), Some(&200));
    }

    #[test]
    fn attached_payload_surfaces_on_open_intent() {
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "minerU", 1_000, 1).unwrap();
        ledger.begin_payout("cashu", "minerU", 1_000).unwrap();
        ledger
            .attach_pending_payload("cashu", "minerU", "minted:TOKEN-1")
            .unwrap();

        let pending = ledger.pending_payouts();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "cashu");
        assert_eq!(pending[0].1, "minerU");
        assert_eq!(pending[0].2, 1_000);
        assert_eq!(pending[0].3.operation_id, "minted:TOKEN-1");
        assert!(!pending[0].3.confirmed);

        // Settling after delivery closes the intent — the payload no
        // longer surfaces.
        ledger
            .settle_payout("cashu", "minerU", 1_000, "minted:TOKEN-1")
            .unwrap();
        assert!(ledger.pending_payouts().is_empty());
    }

    #[test]
    fn stranded_intent_survives_restart_without_payload() {
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-stranded-{}-{}",
            std::process::id(),
            test_dir_nonce()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        {
            let ledger = Ledger::open(path.clone()).unwrap();
            ledger.accrue("lightning", "minerC", 2_000, 1).unwrap();
            // Simulate a crash between begin_payout and settle/rollback.
            ledger.begin_payout("lightning", "minerC", 1_200).unwrap();
        }
        let reopened = Ledger::open(path).unwrap();
        let stranded = reopened.stranded_pending_payouts();
        assert_eq!(
            stranded.len(),
            1,
            "crash between begin and settle must leave a visible stranded intent"
        );
        assert_eq!(stranded[0].1, "minerC");
        assert_eq!(stranded[0].2, 1_200);
        // Balance stays debited — no double-credit, operator reconciles.
        assert_eq!(reopened.balances("lightning").get("minerC"), Some(&800));
    }

    /// A v1 ledger file has no `rail` field and no `idem_key` on
    /// pending payouts; replay must accept it cleanly and key state by
    /// `plugin`.
    #[test]
    fn v1_ledger_file_without_rail_or_idem_replays() {
        let dir = std::env::temp_dir().join(format!(
            "hydrapool-v1-{}-{}",
            std::process::id(),
            test_dir_nonce()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        // Hand-written v1 entries: accrual + open pending debit +
        // confirmed settle, all without `rail` or `idem_key`.
        let v1_lines = [
            serde_json::json!({
                "plugin": "lightning", "miner_id": "v1miner",
                "delta_sats": 5_000, "ts": 1_700_000_000, "block_height": 10,
            })
            .to_string(),
            serde_json::json!({
                "plugin": "lightning", "miner_id": "v1miner",
                "delta_sats": -2_000, "ts": 1_700_000_100, "block_height": 0,
                "pending": {"operation_id": "", "confirmed": false},
            })
            .to_string(),
            serde_json::json!({
                "plugin": "cashu", "miner_id": "v1cashu",
                "delta_sats": 1_000, "ts": 1_700_000_200, "block_height": 11,
            })
            .to_string(),
        ];
        std::fs::write(&path, v1_lines.join("\n") + "\n").unwrap();
        let ledger = Ledger::open(path).unwrap();
        assert_eq!(ledger.balances("lightning").get("v1miner"), Some(&3_000));
        assert_eq!(ledger.balances("cashu").get("v1cashu"), Some(&1_000));
        // The open v1 pending debit still surfaces for reconciliation.
        let pending = ledger.pending_payouts();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "lightning");
        assert_eq!(pending[0].1, "v1miner");
        assert_eq!(pending[0].2, 2_000);
        assert!(pending[0].3.idem_key.is_none());
    }

    #[test]
    fn settle_stamps_idem_key_and_stranded_logs_it() {
        let ledger = tmp_ledger();
        ledger.accrue("fedimint", "minerI", 1_000, 1).unwrap();
        ledger.begin_payout("fedimint", "minerI", 400).unwrap();
        ledger
            .settle_payout_with_idem("fedimint", "minerI", "intent-op", "gateway-op-77")
            .unwrap();
        // Settled — no open intents, and the idem_key is recorded on
        // the (closed) settle entry in the file for audit.
        assert!(ledger.pending_payouts().is_empty());
        let dir = ledger.path.parent().unwrap().to_path_buf();
        let file = std::fs::read_to_string(ledger.path.clone()).unwrap();
        assert!(
            file.contains("\"idem_key\":\"gateway-op-77\""),
            "settle entry must stamp the idem_key; file:\n{file}"
        );
        drop(dir);
    }

    #[test]
    fn new_entries_carry_rail_alias() {
        let ledger = tmp_ledger();
        ledger.accrue("cashu", "minerR", 500, 3).unwrap();
        let file = std::fs::read_to_string(ledger.path.clone()).unwrap();
        assert!(
            file.contains("\"rail\":\"cashu\""),
            "new entries must carry the rail alias; file: {file}"
        );
    }
}
