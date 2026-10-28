use crate::addons::pending_feed::PendingFeed;
use crate::addons::utils::{EthWrapper, new_headers_stream, pipe_from_stream};
use alloy_consensus::transaction::TxHashRef;
use alloy_rpc_types::pubsub::{Params, SubscriptionKind};
use async_trait::async_trait;
use jsonrpsee::PendingSubscriptionSink;
use jsonrpsee::SubscriptionMessage;
use jsonrpsee_types::ErrorObject;
use reth::tasks::TaskSpawner;
use reth_rpc::EthPubSub;
use reth_rpc_convert::RpcTransaction;
use reth_rpc_eth_api::{EthApiTypes, EthPubSubApiServer};
use std::sync::Arc;

pub struct SubscribeFixup<Eth: EthWrapper> {
    pubsub: Arc<EthPubSub<Eth>>,
    provider: Arc<Eth::Provider>,
    subscription_task_spawner: Box<dyn TaskSpawner + 'static>,
    pending_feed: Option<PendingFeed>,
}

#[async_trait]
impl<Eth: EthWrapper> EthPubSubApiServer<RpcTransaction<Eth::NetworkTypes>> for SubscribeFixup<Eth>
where
    ErrorObject<'static>: From<<Eth as EthApiTypes>::Error>,
{
    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        kind: SubscriptionKind,
        params: Option<Params>,
    ) -> jsonrpsee::core::SubscriptionResult {
        let sink = pending.accept().await?;
        let (pubsub, provider) = (self.pubsub.clone(), self.provider.clone());
        let pending_feed = self.pending_feed.clone();
        self.subscription_task_spawner.spawn(Box::pin(async move {
            if kind == SubscriptionKind::NewHeads {
                let _ = pipe_from_stream(sink, new_headers_stream::<Eth>(&provider)).await;
            } else if kind == SubscriptionKind::NewPendingTransactions && pending_feed.is_some() {
                let mut rx = pending_feed.unwrap().subscribe();
                let full = matches!(params, Some(Params::Bool(true)));
                loop {
                    tokio::select! {
                        _ = sink.closed() => break,
                        item = rx.recv() => match item {
                            Ok(tx) => {
                                let msg = if full {
                                    SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &tx)
                                } else {
                                    SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), tx.inner.tx_hash())
                                };
                                if let Ok(msg) = msg {
                                    if sink.send(msg).await.is_err() { break; }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => { tracing::warn!(n, "pending subscriber lagged"); break; }
                            Err(_) => break,
                        }
                    }
                }
            } else {
                let _ = pubsub.handle_accepted(sink, kind, params).await;
            }
        }));
        Ok(())
    }
}

impl<Eth: EthWrapper> SubscribeFixup<Eth> {
    pub fn new(
        pubsub: Arc<EthPubSub<Eth>>,
        provider: Arc<Eth::Provider>,
        subscription_task_spawner: Box<dyn TaskSpawner + 'static>,
        pending_feed: Option<PendingFeed>,
    ) -> Self
    where
        Eth: EthWrapper,
        ErrorObject<'static>: From<Eth::Error>,
    {
        Self { pubsub, provider, subscription_task_spawner, pending_feed }
    }
}
