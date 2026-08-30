use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionConsumerLoopSummary {
    pub polls: usize,
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}
