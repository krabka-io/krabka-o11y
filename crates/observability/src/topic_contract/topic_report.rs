use super::{ObservedTopic, PartitionCount, TopicDrift};

/// What provisioning found. Returned when no fatal drift remains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicReport {
    /// Every topic the call checked, as the broker reports it.
    pub observed: Vec<ObservedTopic>,

    /// Differences that do not stop a role from starting. Callers log these.
    pub advisory: Vec<TopicDrift>,
}

impl TopicReport {
    /// The live shard count for one topic, or `None` when the call did not
    /// check that topic.
    ///
    /// This is the accessor for "how many shards does this signal have". No
    /// setting anywhere else carries that number.
    #[must_use]
    pub fn partitions(&self, topic: &str) -> Option<PartitionCount> {
        self.observed
            .iter()
            .find(|entry| entry.name == topic)
            .map(|entry| entry.partitions)
    }
}
