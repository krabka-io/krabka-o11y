use super::{AttrValue, RecordBatch, TraceqlError, attr_values_with_resource};

pub(crate) fn attr_values(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    attr_values_with_resource(batch, row, false)
}
