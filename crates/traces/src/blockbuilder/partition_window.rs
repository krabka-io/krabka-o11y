use super::{OffsetRange, SpanRecord};

/// Decoded records from one Kafka partition and their inclusive offset range.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionWindow {
    pub offset_range: OffsetRange,
    pub records: Vec<SpanRecord>,
}
