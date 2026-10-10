use arrow::{array::PrimitiveArray, datatypes::Int64Type};

use super::{AsArray, COL_START, RecordBatch, Result, TraceqlError};

/// The span start times of a scanned batch, in Unix nanoseconds.
pub(crate) fn span_start_column(batch: &RecordBatch) -> Result<&PrimitiveArray<Int64Type>> {
    Ok(batch
        .column_by_name(COL_START)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {COL_START}")))?
        .as_primitive::<Int64Type>())
}
