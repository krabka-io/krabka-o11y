use super::{Display, From, Into};

/// A query time offset, in nanoseconds. This is the `offset 1h` in a metric
/// query.
///
/// May be negative, so it is a distinct type from [`DurationNanos`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct OffsetNanos(pub i64);
