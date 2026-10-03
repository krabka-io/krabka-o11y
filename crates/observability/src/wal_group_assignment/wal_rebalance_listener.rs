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
    /// The partitions to rewind, or `None` while rewinding is off. Clones
    /// share it, so a role can turn it on after the consumer took its clone.
    rewind: Arc<Mutex<Option<BTreeSet<i32>>>>,
}

impl WalRebalanceListener {
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            revoked: Arc::default(),
            rewind: Arc::default(),
        }
    }

    /// The same listener, which also rewinds a revoked partition that the
    /// group gives back to this member in the same rebalance.
    ///
    /// An eager rebalance revokes every partition and then assigns most of
    /// them again. A poll loop that fences the buffered records of each
    /// revoked partition loses them: the client keeps the fetch position of a
    /// partition that it gets back, so the member does not read the fenced
    /// records again, and its next commit moves past them. The rewind seeks
    /// such a partition back to the group's committed offset.
    ///
    /// Use this only for a consumer that fences revoked partitions and
    /// commits what it applies. A consumer that never commits would rewind to
    /// the log start and read the whole retained WAL again.
    #[must_use]
    pub fn rewinding_fenced_partitions(self) -> Self {
        lock(&self.rewind).get_or_insert_default();
        self
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

fn lock<T>(partitions: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    partitions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[async_trait]
impl ConsumerRebalanceListener for WalRebalanceListener {
    async fn on_partitions_revoked(
        &mut self,
        _consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError> {
        let revoked: Vec<i32> = partitions
            .iter()
            .filter(|(topic, _)| topic == &self.topic)
            .map(|(_, partition)| *partition)
            .collect();
        lock(&self.revoked).extend(revoked.iter().copied());
        if let Some(rewind) = lock(&self.rewind).as_mut() {
            rewind.extend(revoked);
        }
        Ok(())
    }

    async fn on_partitions_assigned(
        &mut self,
        consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError> {
        // Read the set now and clear it only at the end. A cancelled `poll`
        // can drop this callback at an await, and the client then runs it
        // again with the same partitions.
        let Some(rewind) = lock(&self.rewind).clone() else {
            return Ok(());
        };
        let returned: Vec<(String, i32)> = partitions
            .iter()
            .filter(|(topic, partition)| topic == &self.topic && rewind.contains(partition))
            .cloned()
            .collect();
        if !returned.is_empty() {
            self.rewind_to_committed(consumer, returned).await?;
        }
        if let Some(pending) = lock(&self.rewind).as_mut() {
            pending.retain(|partition| !rewind.contains(partition));
        }
        Ok(())
    }
}

impl WalRebalanceListener {
    /// Moves the position of each partition in `returned` back to the group's
    /// committed offset.
    async fn rewind_to_committed(
        &self,
        consumer: &Consumer,
        returned: Vec<(String, i32)>,
    ) -> Result<(), RebalanceListenerError> {
        let committed = consumer.committed(&returned).await?;
        let mut never_committed = Vec::new();
        for partition in returned {
            match committed.get(&partition).and_then(Option::as_ref) {
                Some(offset) => {
                    consumer
                        .seek(partition.0, partition.1, offset.offset)
                        .await?;
                }
                None => never_committed.push(partition),
            }
        }
        // A partition with no commit replays from the log start. The roles
        // that rewind all reset to the earliest offset, so that is where a
        // fresh member starts too.
        if !never_committed.is_empty() {
            consumer.seek_to_beginning(&never_committed).await?;
        }
        Ok(())
    }
}
