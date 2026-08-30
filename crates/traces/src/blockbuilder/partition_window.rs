use super::SpanRecord;

/// Decoded records from one Kafka partition and their inclusive offset range.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionWindow {
    pub offset_range: (i64, i64),
    pub records: Vec<SpanRecord>,
}
