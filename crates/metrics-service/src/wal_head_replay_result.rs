use super::WalHeadPartitionOffset;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalHeadReplayResult {
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<WalHeadPartitionOffset>,
}
