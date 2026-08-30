use super::{Display, From, Into};

/// An *instant*: an absolute timestamp in Unix epoch nanoseconds.
///
/// The crate uses this type for the query window bounds `start_ns` and
/// `end_ns`, for a span row's start time, and for the per-bucket output
/// timestamps of a metrics range query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub(crate) struct UnixNano(pub i64);
