use super::HaElectionPartitionOffset;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionConsumerLoopSummary {
    pub polls: usize,
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}

impl HaElectionConsumerLoopSummary {
    pub(crate) fn record_poll(
        &mut self,
        polled_records: usize,
        replayed_records: usize,
        committed_offsets: impl IntoIterator<Item = HaElectionPartitionOffset>,
    ) {
        self.polls += 1;
        self.polled_records += polled_records;
        self.replayed_records += replayed_records;
        self.committed_offsets.extend(committed_offsets);
    }
}
