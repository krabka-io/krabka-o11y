use super::{Display, From, Into};

/// A duration in nanoseconds: a *span of time*, not an instant.
///
/// The crate uses this type for the metrics range-query `step`. It stays
/// distinct from [`UnixNano`], so a step can never take a timestamp position,
/// and a timestamp can never take a step position. This holds for the
/// metrics-range structs and for the `assemble_*` arg lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub(crate) struct DurationNanos(pub i64);
