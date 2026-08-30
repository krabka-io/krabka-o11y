use super::*;

/// Request-body bytes accepted on the ingest path, for the cumulative bytes
/// counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct IngestBytes(pub u64);
