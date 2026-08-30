use super::{Array, FixedSizeBinaryArray, RecordBatch, TraceqlError};

pub(crate) fn fixed<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, TraceqlError> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| TraceqlError::Store(format!("missing fixed binary column `{name}`")))
}
