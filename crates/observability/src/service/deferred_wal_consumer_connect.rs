use super::ClientResourcePolicy;

/// Parameters needed to connect a [`KafkaLogWalConsumer`] in the background.
#[derive(Clone)]
pub(crate) struct DeferredWalConsumerConnect {
    pub(crate) bootstrap: String,
    pub(crate) group_id: String,
    pub(crate) topic: String,
    pub(crate) client_resource_policy: ClientResourcePolicy,
}
