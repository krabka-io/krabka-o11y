use super::{
    BTreeMap, CompactionConsumerCommit, CompactionConsumerCommitError, CompactionConsumerCommitMut,
    CompactionConsumerPoll, CompactionConsumerPollError, CompactionPartitionOffset, ConsumerRecord,
    PartitionIndex, Time, async_trait,
};

/// Polling consumer that recommits every highest durable partition offset.
pub struct DurableCompactionConsumer<C> {
    inner: C,
    topic: String,
    durable_offsets: BTreeMap<PartitionIndex, CompactionPartitionOffset>,
}

impl<C> DurableCompactionConsumer<C> {
    #[must_use]
    pub fn new(inner: C, topic: impl Into<String>) -> Self {
        Self {
            inner,
            topic: topic.into(),
            durable_offsets: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl<C> CompactionConsumerPoll for DurableCompactionConsumer<C>
where
    C: CompactionConsumerPoll + Send,
{
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError> {
        self.inner.poll(timeout).await
    }
}

#[async_trait]
impl<C> CompactionConsumerCommitMut for DurableCompactionConsumer<C>
where
    C: CompactionConsumerCommit + Send,
{
    async fn commit_offsets_sync_mut(
        &mut self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionConsumerCommitError> {
        let assigned = self.inner.assignment().await;
        let assigned = assigned
            .into_iter()
            .filter_map(|(topic, partition)| {
                (topic == self.topic).then_some(PartitionIndex(partition))
            })
            .collect::<std::collections::BTreeSet<_>>();
        self.durable_offsets
            .retain(|partition, _| assigned.contains(partition));
        for offset in offsets {
            if !assigned.contains(&offset.partition) {
                continue;
            }
            self.durable_offsets
                .entry(offset.partition)
                .and_modify(|current| {
                    if offset.offset > current.offset {
                        *current = offset.clone();
                    }
                })
                .or_insert_with(|| offset.clone());
        }
        self.inner
            .commit_offsets_sync(
                &self.topic,
                &self.durable_offsets.values().cloned().collect::<Vec<_>>(),
            )
            .await
    }
}
