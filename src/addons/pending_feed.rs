//! Parsing of pre-consensus EVM transactions from hl-node gossip records.
//!
//! Parsing a transaction does not mean it was accepted or will be included.

use alloy_eips::Decodable2718;
use alloy_primitives::Bytes;
use alloy_rpc_types::{Transaction, TransactionInfo};
use reth_primitives_traits::SignerRecoverable;
use reth_rpc_eth_api::transaction::FromConsensusTx;
use serde::Deserialize;

use crate::addons::utils::EthWrapper;
use crate::node::primitives::transaction::TransactionSigned;
use alloy_consensus::transaction::TxHashRef;
use alloy_primitives::B256;
use jsonrpsee::{
    core::{RpcResult, async_trait},
    proc_macros::rpc,
};
use reth_rpc_eth_api::EthApiServer;
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::broadcast;

#[rpc(server, namespace = "eth")]
pub trait PendingLookupApi {
    #[method(name = "getTransactionByHash")]
    async fn get_transaction_by_hash(&self, hash: B256) -> RpcResult<Option<Transaction>>;
}

pub struct PendingLookup<Eth: EthWrapper> {
    pub eth: Eth,
    pub feed: PendingFeed,
}

#[async_trait]
impl<Eth: EthWrapper> PendingLookupApiServer for PendingLookup<Eth> {
    async fn get_transaction_by_hash(&self, hash: B256) -> RpcResult<Option<Transaction>> {
        if let Some(tx) = EthApiServer::transaction_by_hash(&self.eth, hash).await? {
            return Ok(Some(tx));
        }
        Ok(self.feed.get(hash))
    }
}

#[derive(Clone)]
pub struct PendingFeed {
    cache: Arc<Mutex<(HashMap<B256, Transaction>, VecDeque<B256>)>>,
    sender: broadcast::Sender<Transaction>,
}

impl PendingFeed {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(8192);
        Self { cache: Arc::new(Mutex::new((HashMap::new(), VecDeque::new()))), sender }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Transaction> {
        self.sender.subscribe()
    }
    pub fn get(&self, hash: B256) -> Option<Transaction> {
        self.cache.lock().unwrap().0.get(&hash).cloned()
    }
    fn insert(&self, tx: Transaction) {
        let hash = *tx.inner.tx_hash();
        let mut cache = self.cache.lock().unwrap();
        if cache.0.contains_key(&hash) {
            return;
        }
        if cache.1.len() >= 100_000 {
            if let Some(old) = cache.1.pop_front() {
                cache.0.remove(&old);
            }
        }
        cache.1.push_back(hash);
        cache.0.insert(hash, tx.clone());
        drop(cache);
        let _ = self.sender.send(tx);
    }
    pub fn start(self, directory: PathBuf) {
        tokio::spawn(async move {
            use std::io::SeekFrom;
            use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
            let mut current = PathBuf::new();
            let mut reader: Option<BufReader<tokio::fs::File>> = None;
            let mut partial = Vec::new();
            let mut first = true;
            loop {
                let next = newest_file(&directory);
                if let Some(path) = next {
                    if reader.is_none() {
                        match tokio::fs::File::open(&path).await {
                            Ok(mut file) => {
                                if first {
                                    let _ = file.seek(SeekFrom::End(0)).await;
                                }
                                first = false;
                                current = path;
                                reader = Some(BufReader::new(file));
                            }
                            Err(e) => tracing::warn!(?e, ?path, "pending file open failed"),
                        }
                    }
                }
                if let Some(r) = reader.as_mut() {
                    let mut chunk = Vec::new();
                    match r.read_until(b'\n', &mut chunk).await {
                        Ok(0) => {
                            if partial.is_empty()
                                && newest_file(&directory).as_ref().is_some_and(|p| *p != current)
                            {
                                reader = None;
                            } else {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                        Ok(_) => {
                            partial.extend_from_slice(&chunk);
                            if partial.len() > 16 * 1024 * 1024 {
                                tracing::warn!("pending record exceeds limit");
                                partial.clear();
                            } else if partial.last() == Some(&b'\n') {
                                if let Ok(txs) = parse_record(&partial) {
                                    for tx in txs {
                                        self.insert(tx);
                                    }
                                }
                                partial.clear();
                            }
                        }
                        Err(e) => {
                            tracing::warn!(?e, "pending file read failed");
                            reader = None;
                            current = PathBuf::new();
                        }
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        });
    }
}

fn newest_file(root: &Path) -> Option<PathBuf> {
    let day = std::fs::read_dir(root.join("hourly"))
        .ok()?
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .max_by_key(|e| e.file_name())?;
    std::fs::read_dir(day.path())
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|e| e.file_name().to_str()?.parse::<u8>().ok().map(|h| (h, e.path())))
        .max_by_key(|(h, _)| *h)
        .map(|(_, p)| p)
}

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

    #[test]
    fn decodes_signed_evm_action() {
        let raw = "0x02f901148203e783015537830f4240849502f9008308c3ab9400a89d7a5a02160f20150ebea7a2b5e4879a1a8b80b8a4a415bcad000000000000000000000000b88339cb7199b77e23db6e890353e22632ba630f0000000000000000000000000000000000000000000000000000000077077424000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000009a1dbc6a6e7e59f6beb23db0d00b603fc90e94d0c080a08939691da80a1944c2c3a3a4d4b1afb3fd42f4fae428e0c7b0fa793745be7555a0742152b667f0e8cc26f8ad1b7c7aac4065d0b15e1ad123f1fdeffe228ebdc8d6";
        let record = format!(
            r#"["2026-10-28",{{"signed_actions":[{{"action":{{"type":"evmRawTx","data":"{raw}"}}}}]}}]"#
        );
        let txs = parse_record(record.as_bytes()).unwrap();
        assert_eq!(txs.len(), 1);
        assert!(txs[0].block_hash.is_none());
        let feed = PendingFeed::new();
        feed.insert(txs[0].clone());
        assert!(feed.get(*txs[0].inner.tx_hash()).is_some());
    }
}
