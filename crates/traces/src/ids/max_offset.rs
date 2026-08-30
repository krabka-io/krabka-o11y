use super::{Display, From, Into};

/// The largest Kafka log offset covered by a flushed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MaxOffset(pub i64);
