use super::{Display, From, Into};

/// A wall-clock timestamp in nanoseconds since the Unix epoch.
///
/// This carries the `start` and `end` bounds of a time range. They are adjacent
/// `i64`s at every call site that filters or steps over a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct UnixNano(pub i64);
