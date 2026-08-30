use super::{Display, From, Into};

/// A query-window start bound, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct StartMs(pub i64);
