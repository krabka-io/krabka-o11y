use std::collections::BTreeSet;

/// Storage metadata used to estimate the cost of a profile query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileQueryStats {
    pub block_count: u64,
    pub fingerprints: BTreeSet<u64>,
    pub profile_count: u64,
    pub sample_count: u64,
}
