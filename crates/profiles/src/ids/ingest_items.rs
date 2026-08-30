use super::*;

/// Profile/sample items ingested, for the cumulative items counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct IngestItems(pub u64);
