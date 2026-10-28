//! Parsing of pre-consensus EVM transactions from hl-node gossip records.
//!
//! This module is not wired into RPC yet. In particular, parsing a transaction
//! does not mean it was accepted by HyperCore or will be included in a block.

use alloy_eips::Decodable2718;
use alloy_primitives::Bytes;
use alloy_rpc_types::{Transaction, TransactionInfo};
use reth_primitives_traits::SignerRecoverable;
use reth_rpc_eth_api::transaction::FromConsensusTx;
use serde::Deserialize;

use crate::node::primitives::transaction::TransactionSigned;

#[derive(Deserialize)]
struct GossipRecord(String, GossipBundle);

#[derive(Deserialize)]
struct GossipBundle {
    signed_actions: Vec<SignedAction>,
}

#[derive(Deserialize)]
struct SignedAction {
    action: Action,
}

#[derive(Deserialize)]
struct Action {
    #[serde(rename = "type")]
    kind: String,
    data: Option<String>,
}

/// Decode valid EVM envelopes from one complete newline-delimited gossip record.
pub fn parse_record(line: &[u8]) -> Result<Vec<Transaction>, serde_json::Error> {
    let GossipRecord(_timestamp, bundle) = serde_json::from_slice(line)?;
    Ok(bundle
        .signed_actions
        .into_iter()
        .filter_map(|action| {
            if action.action.kind != "evmRawTx" {
                return None;
            }
            let bytes: Bytes = action.action.data?.parse().ok()?;
            let mut input = bytes.as_ref();
            let tx = TransactionSigned::decode_2718(&mut input).ok()?;
            if !input.is_empty() {
                return None;
            }
            let signer = tx.recover_signer().ok()?;
            Transaction::from_consensus_tx(tx, signer, TransactionInfo::default()).ok()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_other_actions_and_invalid_evm_data() {
        let record = br#"["2026-10-28T07:59:59",{"signed_actions":[{"action":{"type":"order"}},{"action":{"type":"evmRawTx","data":"0x1234"}}]}]"#;
        assert!(parse_record(record).unwrap().is_empty());
    }

    #[test]
    fn rejects_incomplete_records() {
        assert!(parse_record(br#"["2026-10-28"#).is_err());
    }
}
