use super::{BoxStream, RecordBatch, Result};

/// One compaction input, as the batches it holds rather than as the batches
/// themselves.
///
/// Boxed because the compactors put their own per-batch work in front of the
/// reader -- traces widen each batch to the merged promoted-attribute schema,
/// profiles rewrite its stacktrace partitions -- and the merge only needs
/// something that yields batches in the declared order.
pub type BlockBatchStream = BoxStream<'static, Result<RecordBatch>>;
