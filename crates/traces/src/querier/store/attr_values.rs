use super::{Attribute, RecordBatch, ResourceAttrs, TraceqlError, attr_values_with_resource};

pub(crate) fn attr_values(batch: &RecordBatch, row: usize) -> Result<Vec<Attribute>, TraceqlError> {
    attr_values_with_resource(batch, row, ResourceAttrs::Exclude)
}
