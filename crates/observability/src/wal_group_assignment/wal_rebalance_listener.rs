use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use krabka_client_consumer::{Consumer, ConsumerRebalanceListener, RebalanceListenerError};

/// Records partitions revoked while [`Consumer::poll`] applies a rebalance.
#[derive(Clone, Debug)]
pub struct WalRebalanceListener {
    topic: String,
    revoked: Arc<Mutex<BTreeSet<i32>>>,
}

impl WalRebalanceListener {
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            revoked: Arc::default(),
        }
    }

    /// Drains the partitions whose buffered, uncommitted records must be replayed.
    pub fn take_revoked_partitions(&self) -> BTreeSet<i32> {
        std::mem::take(
            &mut *self
                .revoked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

#[async_trait]
impl ConsumerRebalanceListener for WalRebalanceListener {
    async fn on_partitions_revoked(
        &mut self,
        _consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError> {
        self.revoked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(
                partitions
                    .iter()
                    .filter(|(topic, _)| topic == &self.topic)
                    .map(|(_, partition)| *partition),
            );
        Ok(())
    }

    async fn on_partitions_assigned(
        &mut self,
        _consumer: &Consumer,
        _partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError> {
        Ok(())
    }
}
