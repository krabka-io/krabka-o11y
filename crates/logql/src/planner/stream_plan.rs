use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct StreamPlan {
    pub tenant: String,
    pub time_range: TimeRange,
    pub query: StreamQuery,
    pub fingerprints: BTreeSet<SeriesFingerprint>,
    pub blocks: Vec<BlockDescriptor>,
}
