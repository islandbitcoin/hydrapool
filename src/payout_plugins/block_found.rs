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

//! Block-found gating for payout-plugin accrual.
//!
//! bitcoind's `zmqpubhashblock` publishes a message for EVERY block
//! accepted by the network, not just blocks this pool found. The
//! plugin accrual must therefore gate on observables that distinguish
//! a pool block before distributing anything:
//!
//! 1. The block's coinbase scriptSig ends with this pool's configured
//!    `pool_signature` — p2poolv2_lib's coinbase builder always pushes
//!    it last (`build_bitcoin_coinbase_transaction`).
//! 2. The coinbase outputs sum exactly to the template's
//!    `coinbasevalue` — the output distribution (donation cut, fee
//!    cut, proportional remainder) is built once per template and sums
//!    to exactly that value.
//!
//! When no pool signature is configured, the gate falls back to the
//! parent-link check: the block's previous hash must equal the
//! template's `previousblockhash`.
//!
//! The subscription itself is a dedicated ZMQ SUB socket here (the
//! lib's `ZmqListener` discards the hash), forwarding the raw 32-byte
//! block hash per message. The found block is fetched over RPC and
//! identified by its real hash, which is also the dedup key: one pool
//! block accrues exactly once.

use p2poolv2_lib::stratum::work::block_template::BlockTemplate;
use tracing::error;

/// Number of recent blocks tracked for duplicate-accrual protection.
const RECENT_BLOCK_CACHE: usize = 8;

/// Number of recent templates kept for block matching. A found block
/// may race a newer template into the watch channel (gbt polls on the
/// same zmq trigger), so the gate checks the template history.
const RECENT_TEMPLATE_CACHE: usize = 8;

/// Subscribe directly to bitcoind's `zmqpubhashblock` topic and forward
/// the raw 32-byte block hash of every announced network block.
///
/// The lib's `ZmqListener` discards the hash (it sends `()`), which is
/// why this socket exists: without the hash the plugin task cannot
/// fetch the block and tell a pool block from a foreign one.
pub fn start_hashblock_listener(
    address: &str,
) -> Result<tokio::sync::mpsc::Receiver<[u8; 32]>, String> {
    let context = zmq::Context::new();
    let socket = context
        .socket(zmq::SUB)
        .map_err(|e| format!("Failed to create ZMQ socket: {e:?}"))?;
    socket
        .set_subscribe(b"hashblock")
        .map_err(|e| format!("Failed to set ZMQ subscription: {e}"))?;
    socket
        .connect(address)
        .map_err(|e| format!("Failed to connect ZMQ socket: {e}"))?;

    let (tx, rx) = tokio::sync::mpsc::channel::<[u8; 32]>(1);
    std::thread::spawn(move || {
        loop {
            // Multipart layout, same as the lib's listener parses:
            // [topic "hashblock", 32-byte block hash, 4-byte sequence].
            match socket.recv_multipart(0) {
                Ok(parts) => {
                    if parts.len() == 3 && parts[1].len() == 32 {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(&parts[1]);
                        if tx.blocking_send(hash).is_err() {
                            break; // receiver gone — pool shutting down
                        }
                    }
                }
                Err(e) => error!("Failed to receive ZMQ hashblock message: {e}"),
            }
        }
    });
    Ok(rx)
}

/// Fetch and deserialize the block for a zmq hashblock notification.
///
/// Bitcoin Core's zmq payload byte order need not match the RPC hash
/// form, so the payload hex is tried as-is first, then byte-reversed.
/// Returns `None` when neither form resolves to a fetchable block.
pub async fn fetch_block_by_hash(
    rpc: &bitcoindrpc::BitcoindRpcClient,
    hash_bytes: &[u8; 32],
) -> Option<bitcoin::Block> {
    let mut reversed = *hash_bytes;
    reversed.reverse();
    for hash_hex in [hex::encode(hash_bytes), hex::encode(reversed)] {
        let raw: String = match rpc
            .request(
                "getblock",
                vec![
                    serde_json::Value::String(hash_hex.clone()),
                    serde_json::json!(0), // verbosity 0: raw block hex
                ],
            )
            .await
        {
            Ok(raw) => raw,
            Err(_) => continue,
        };
        let Ok(bytes) = hex::decode(&raw) else {
            continue;
        };
        if let Ok(block) = bitcoin::consensus::deserialize::<bitcoin::Block>(&bytes) {
            return Some(block);
        }
    }
    None
}

/// Gate for plugin accrual: does this network block carry THIS pool's
/// coinbase, built from `template`'s output distribution?
///
/// A block mined from the template carries the pool's signature as the
/// final push of the coinbase scriptSig, and its coinbase outputs sum
/// exactly to the template's coinbase value (donation and fee cuts are
/// carved out of the same total). Foreign blocks fail one or both.
pub fn is_pool_block(
    block: &bitcoin::Block,
    template: &BlockTemplate,
    pool_signature: Option<&[u8]>,
) -> bool {
    let Some(coinbase) = block.txdata.first() else {
        return false;
    };
    if !coinbase.is_coinbase() {
        return false;
    }
    let total: u64 = coinbase.output.iter().map(|o| o.value.to_sat()).sum();
    if total != template.coinbasevalue {
        return false;
    }
    match pool_signature {
        Some(sig) if !sig.is_empty() => {
            // The signature push is the last thing in the scriptSig.
            coinbase.input[0].script_sig.as_bytes().ends_with(sig)
        }
        // Anonymous mining: no signature to look for. Fall back to the
        // parent link — weaker, but the value check above still holds.
        _ => block.header.prev_blockhash.to_string() == template.previousblockhash,
    }
}

/// Record `hash` as processed; returns false when it was already seen,
/// so a zmq retry can never accrue the same pool block twice.
pub(crate) fn accrue_once(
    recent: &mut std::collections::VecDeque<bitcoin::BlockHash>,
    hash: bitcoin::BlockHash,
) -> bool {
    if recent.contains(&hash) {
        return false;
    }
    recent.push_back(hash);
    while recent.len() > RECENT_BLOCK_CACHE {
        recent.pop_front();
    }
    true
}

/// Keep the template history bounded (see [`RECENT_TEMPLATE_CACHE`]).
pub(crate) fn remember_template(
    recent: &mut std::collections::VecDeque<std::sync::Arc<BlockTemplate>>,
    template: std::sync::Arc<BlockTemplate>,
) {
    recent.push_back(template);
    while recent.len() > RECENT_TEMPLATE_CACHE {
        recent.pop_front();
    }
}

/// Mirror of p2poolv2_lib's `include_address_and_cut`: the sats cut
/// from `amount_sats` at `bips` basis points when `address` is set.
///
/// The lib only cuts when BOTH the address and a positive bip value
/// are configured, and leaves the amount intact when the checked math
/// overflows — this mirrors all three behaviors.
pub(crate) fn bips_cut(
    amount_sats: u64,
    address: Option<&bitcoin::Address>,
    bips: Option<u16>,
) -> u64 {
    const BASIS_POINT_FACTOR: u64 = 10_000;
    match (address, bips.filter(|b| *b > 0)) {
        (Some(_), Some(b)) => u64::checked_mul(amount_sats, u64::from(b))
            .and_then(|v| v.checked_div(BASIS_POINT_FACTOR))
            .unwrap_or_else(|| {
                tracing::warn!(
                    amount_sats,
                    bips = b,
                    "bips cut overflowed — no cut applied"
                );
                0
            }),
        _ => 0,
    }
}

/// The miner-attributable portion of a found block's coinbase.
///
/// The on-chain coinbase builder (p2poolv2_lib
/// `get_output_distribution`) cuts donation bips from the coinbase
/// value first, then fee bips from the remainder, and splits only what
/// is left across miners. The plugin accrual must split the same
/// remainder — accruing the full `coinbasevalue` would pay miners the
/// donation and fee too.
pub fn miner_attributable_sats(
    coinbase_sats: u64,
    donation: Option<u16>,
    donation_address: Option<&bitcoin::Address>,
    fee: Option<u16>,
    fee_address: Option<&bitcoin::Address>,
) -> u64 {
    let donation_cut = bips_cut(coinbase_sats, donation_address, donation);
    let after_donation = coinbase_sats.saturating_sub(donation_cut);
    let fee_cut = bips_cut(after_donation, fee_address, fee);
    after_donation.saturating_sub(fee_cut)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::{Hash, sha256d};
    use bitcoin::{
        Amount, BlockHash, ScriptBuf, Sequence, TxIn, TxOut, absolute, block, transaction,
    };
    use std::sync::Arc;

    const POOL_SIG: &[u8] = b"hydrapool";

    fn template(coinbasevalue: u64, prevhash: BlockHash) -> BlockTemplate {
        BlockTemplate {
            version: 0x2000_0000,
            rules: vec![],
            vbavailable: std::collections::HashMap::new(),
            vbrequired: 0,
            previousblockhash: prevhash.to_string(),
            transactions: vec![],
            coinbaseaux: std::collections::HashMap::new(),
            coinbasevalue,
            longpollid: String::new(),
            target: String::new(),
            mintime: 0,
            mutable: vec![],
            noncerange: String::new(),
            sigoplimit: 0,
            sizelimit: 0,
            weightlimit: 0,
            curtime: 0,
            bits: "1d00ffff".to_string(),
            height: 900,
            default_witness_commitment: None,
        }
    }

    /// A block whose coinbase ends with `sig` and whose single output
    /// pays `coinbase_value` — the shape of a pool-mined block.
    fn block_with_coinbase(coinbase_value: u64, sig: &[u8], prevhash: BlockHash) -> bitcoin::Block {
        let mut sig_buf = bitcoin::script::PushBytesBuf::new();
        sig_buf.extend_from_slice(sig).unwrap();
        let script_sig = bitcoin::script::Builder::new()
            .push_int(100)
            .push_slice([0u8; 8]) // nsecs
            .push_slice(sig_buf)
            .into_script();
        let coinbase = bitcoin::Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig,
                sequence: Sequence::MAX,
                witness: Vec::<Vec<u8>>::new().into(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(coinbase_value),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        bitcoin::Block {
            header: block::Header {
                version: block::Version::TWO,
                prev_blockhash: prevhash,
                merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                time: 0,
                bits: bitcoin::pow::CompactTarget::from_consensus(0x1d00ffff),
                nonce: 0,
            },
            txdata: vec![coinbase],
        }
    }

    fn test_prevhash() -> BlockHash {
        BlockHash::from_raw_hash(sha256d::Hash::from_byte_array([7u8; 32]))
    }

    /// The gate must pass a block whose coinbase carries the pool
    /// signature and the template's exact coinbase value.
    #[test]
    fn pool_block_passes_the_gate() {
        let prevhash = test_prevhash();
        let template = template(3_125_000_000, prevhash);
        let block = block_with_coinbase(3_125_000_000, POOL_SIG, prevhash);
        assert!(is_pool_block(&block, &template, Some(POOL_SIG)));
    }

    /// A foreign block (someone else's signature) accrues nothing.
    #[test]
    fn foreign_block_fails_the_gate() {
        let prevhash = test_prevhash();
        let template = template(3_125_000_000, prevhash);
        let block = block_with_coinbase(3_125_000_000, b"otherpool", prevhash);
        assert!(!is_pool_block(&block, &template, Some(POOL_SIG)));
    }

    /// A coinbase whose outputs do not sum to the template's coinbase
    /// value must not accrue — e.g. a block from a different template.
    #[test]
    fn value_mismatch_fails_the_gate() {
        let prevhash = test_prevhash();
        let template = template(3_125_000_000, prevhash);
        let block = block_with_coinbase(3_000_000_000, POOL_SIG, prevhash);
        assert!(!is_pool_block(&block, &template, Some(POOL_SIG)));
    }

    /// A block whose coinbase lacks any signature push must not match
    /// when a pool signature is configured.
    #[test]
    fn unsigned_coinbase_fails_the_gate() {
        let prevhash = test_prevhash();
        let template = template(3_125_000_000, prevhash);
        let block = block_with_coinbase(3_125_000_000, &[], prevhash);
        assert!(!is_pool_block(&block, &template, Some(POOL_SIG)));
    }

    /// Without a configured signature the gate falls back to the
    /// parent-link check.
    #[test]
    fn anonymous_mode_gates_on_parent_link() {
        let prevhash = test_prevhash();
        let template = template(3_125_000_000, prevhash);
        let pool_block = block_with_coinbase(3_125_000_000, &[], prevhash);
        assert!(is_pool_block(&pool_block, &template, None));

        let other_prev = BlockHash::from_raw_hash(sha256d::Hash::from_byte_array([9u8; 32]));
        let foreign = block_with_coinbase(3_125_000_000, &[], other_prev);
        assert!(!is_pool_block(&foreign, &template, None));
    }

    /// The dedup ring: the same block hash accrues exactly once, and
    /// old hashes fall out of the cache.
    #[test]
    fn accrue_once_dedups_by_block_hash() {
        let mut recent = std::collections::VecDeque::new();
        let hash = test_prevhash();
        assert!(accrue_once(&mut recent, hash));
        assert!(
            !accrue_once(&mut recent, hash),
            "second trigger for the same block must not accrue again"
        );
        // Older entries are evicted, so a hash can accrue again after
        // RECENT_BLOCK_CACHE distinct blocks in between.
        for i in 0..RECENT_BLOCK_CACHE {
            let mut bytes = [0u8; 32];
            bytes[0] = i as u8 + 1;
            assert!(accrue_once(
                &mut recent,
                BlockHash::from_raw_hash(sha256d::Hash::from_byte_array(bytes))
            ));
        }
        assert!(!recent.contains(&hash), "cache must be bounded");
    }

    #[test]
    fn template_history_is_bounded() {
        let mut recent = std::collections::VecDeque::new();
        for i in 0..(RECENT_TEMPLATE_CACHE * 3) {
            let prev = BlockHash::from_raw_hash(sha256d::Hash::from_byte_array([i as u8; 32]));
            remember_template(&mut recent, Arc::new(template(100, prev)));
        }
        assert_eq!(recent.len(), RECENT_TEMPLATE_CACHE);
    }

    // --- donation/fee cut parity with the lib's coinbase builder ---

    const ADDR: &str = "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq";
    fn addr() -> bitcoin::Address {
        ADDR.parse::<bitcoin::Address<_>>()
            .unwrap()
            .assume_checked()
    }

    #[test]
    fn no_cuts_when_unconfigured() {
        let v = 3_125_000_000;
        assert_eq!(miner_attributable_sats(v, None, None, None, None), v);
        // bips set but address missing — the lib does not cut.
        assert_eq!(
            miner_attributable_sats(v, Some(100), None, Some(200), None),
            v
        );
        // zero bips never cut, same as the lib's filter.
        assert_eq!(
            miner_attributable_sats(v, Some(0), Some(&addr()), Some(0), Some(&addr())),
            v
        );
    }

    #[test]
    fn donation_and_fee_cuts_match_the_coinbase_builder() {
        let v = 3_125_000_000u64;
        // 100 bips = 1%: 31_250_000 sats.
        assert_eq!(bips_cut(v, Some(&addr()), Some(100)), 31_250_000);
        // Donation first, then fee from the remainder.
        let after_donation = v - 31_250_000;
        assert_eq!(
            miner_attributable_sats(v, Some(100), Some(&addr()), Some(200), Some(&addr())),
            after_donation - after_donation * 200 / 10_000,
        );
    }

    /// Miner payouts plus the configured cuts must equal the coinbase
    /// value exactly — nothing minted out of thin air, nothing lost.
    #[test]
    fn miners_get_coinbase_minus_cuts() {
        let v = 1_000_000u64;
        let got = miner_attributable_sats(v, Some(1_000), Some(&addr()), Some(500), Some(&addr()));
        // 10% donation = 100_000; fee 5% of the 900_000 remainder = 45_000.
        assert_eq!(got, 855_000);
    }

    /// The shipped config's 100% donation must leave zero accrual —
    /// otherwise miners would be paid out of the pool's own funds.
    #[test]
    fn full_donation_accrues_zero() {
        let v = 3_125_000_000u64;
        assert_eq!(
            miner_attributable_sats(v, Some(10_000), Some(&addr()), None, None),
            0
        );
        // Even with a fee configured, 100% donation leaves nothing.
        assert_eq!(
            miner_attributable_sats(v, Some(10_000), Some(&addr()), Some(200), Some(&addr())),
            0,
        );
    }
}
