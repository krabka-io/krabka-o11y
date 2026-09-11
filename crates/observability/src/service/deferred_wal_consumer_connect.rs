use super::ClientResourcePolicy;
use crate::wal_consumer_metrics::WalConsumerMetrics;

/// Parameters needed to connect a [`KafkaLogWalConsumer`] in the background.
#[derive(Clone)]
pub(crate) struct DeferredWalConsumerConnect {
    pub(crate) bootstrap: String,
    pub(crate) group_id: String,
    pub(crate) topic: String,
    pub(crate) client_resource_policy: ClientResourcePolicy,
    /// The instruments the connected consumer records its polls into. They
    /// travel with the connection parameters because the consumer is built in
    /// a background task, long after the role has its registry.
    pub(crate) metrics: WalConsumerMetrics,
}
