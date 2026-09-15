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
    plugin: String,
    miner_id: String,
    /// Delta in sats: positive on accrual, negative on payout.
    delta_sats: i64,
    /// Unix seconds.
    ts: u64,
    /// Height of the block that triggered the entry (0 for payouts).
    block_height: u32,
}

/// In-memory ledger state: plugin -> miner_id -> accrued sats.
#[derive(Default)]
pub struct Ledger {
    path: PathBuf,
    state: Mutex<HashMap<(String, String), i64>>,
}

impl Ledger {
    /// Open (or create) the ledger at `path`, replaying any existing
    /// entries. Corrupt trailing lines (torn writes) are skipped.
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
                *state
                    .entry((entry.plugin, entry.miner_id))
                    .or_insert(0) += entry.delta_sats;
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
    pub fn accrue(
        &self,
        plugin: &str,
        miner_id: &str,
        sats: u64,
        block_height: u32,
    ) -> std::io::Result<()> {
        self.append(LedgerEntry {
            plugin: plugin.to_string(),
            miner_id: miner_id.to_string(),
            delta_sats: sats as i64,
            ts: now(),
            block_height: block_height,
        })
    }

    /// Record a payout of `sats` (a negative delta). Returns the
    /// remaining balance, or an error if the payout exceeds the balance.
    pub fn record_payout(
        &self,
        plugin: &str,
        miner_id: &str,
        sats: u64,
    ) -> Result<u64, String> {
        let entry = LedgerEntry {
            plugin: plugin.to_string(),
            miner_id: miner_id.to_string(),
            delta_sats: -(sats as i64),
            ts: now(),
            block_height: 0,
        };
        // Compute + append while holding the lock once — `append` also
        // locks, so releasing first would race with a concurrent payout.
        let balance = {
            let mut state = self.state.lock().unwrap();
            let key = (plugin.to_string(), miner_id.to_string());
            let balance = state.get(&key).copied().unwrap_or(0);
            if (sats as i64) > balance {
                return Err(format!(
                    "payout {sats} sats exceeds accrued {balance} sats for {miner_id}"
                ));
            }
            let new_balance = balance - sats as i64;
            self.append_entry(&mut state, entry).map_err(|e| e.to_string())?;
            new_balance
        };
        Ok(balance as u64)
    }

    /// Peek balances for one plugin without mutating.
    pub fn balances(&self, plugin: &str) -> HashMap<String, u64> {
        self.state
            .lock()
            .unwrap()
            .iter()
            .filter(|((p, _), v)| p == plugin && **v > 0)
            .map(|((_, m), v)| (m.clone(), *v as u64))
            .collect()
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
        *state
            .entry((entry.plugin, entry.miner_id))
            .or_insert(0) += entry.delta_sats;
        Ok(())
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Convert raw balances into [`MinerBalance`] list, dropping zero entries.
pub fn to_miner_balances(balances: HashMap<String, u64>) -> Vec<MinerBalance> {
    balances
        .into_iter()
        .filter(|(_, sats)| *sats > 0)
        .map(|(miner_id, sats)| MinerBalance { miner_id, sats })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_ledger() -> Ledger {
        let dir = std::env::temp_dir().join(format!("hydrapool-ledger-{}", std::process::id()));
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
        let remaining = ledger.record_payout("cashu", "npub123", 7_500).unwrap();
        assert_eq!(remaining, 0);
        assert!(ledger.balances("cashu").get("npub123").is_none());
    }

    #[test]
    fn payout_over_balance_rejected() {
        let ledger = tmp_ledger();
        ledger.accrue("lightning", "worker1", 100, 900).unwrap();
        assert!(ledger.record_payout("lightning", "worker1", 101).is_err());
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
        let dir = std::env::temp_dir().join(format!("hydrapool-replay-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        {
            let ledger = Ledger::open(path.clone()).unwrap();
            ledger.accrue("cashu", "npubX", 1_000, 10).unwrap();
            ledger.record_payout("cashu", "npubX", 400).unwrap();
        }
        let reopened = Ledger::open(path).unwrap();
        assert_eq!(reopened.balances("cashu").get("npubX"), Some(&600));
    }

    #[test]
    fn torn_final_line_is_skipped() {
        let dir = std::env::temp_dir().join(format!("hydrapool-torn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.jsonl");
        let ledger = Ledger::open(path.clone()).unwrap();
        ledger.accrue("cashu", "npubY", 900, 3).unwrap();
        drop(ledger);
        // Simulate a torn write: append a partial JSON line.
        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            use std::io::Write;
            write!(f, "{{\"plugin\":\"ca").unwrap();
        }
        let reopened = Ledger::open(path).unwrap();
        assert_eq!(reopened.balances("cashu").get("npubY"), Some(&900));
    }
}
