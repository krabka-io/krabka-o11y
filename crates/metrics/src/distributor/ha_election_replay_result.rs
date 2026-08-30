use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionReplayResult {
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}
