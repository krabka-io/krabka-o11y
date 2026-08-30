use super::{MetricBlockKind, PartitionIndex, RecordBatch, CompactionSeriesLabels};

pub(crate) struct CompactedBlockRequest<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) kind: MetricBlockKind,
    pub(crate) partition: Option<PartitionIndex>,
    pub(crate) first_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) batch: RecordBatch,
    pub(crate) series: Vec<CompactionSeriesLabels>,
}
