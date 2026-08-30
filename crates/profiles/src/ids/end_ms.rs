use super::{Display, From, Into};

/// A query-window end bound, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct EndMs(pub i64);
