use arrow::{
    array::{Array, AsArray, BinaryArray},
    datatypes::{Int64Type, UInt64Type},
    record_batch::RecordBatch,
};

use super::ProfileError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SelectedSampleRow {
    pub partition: u64,
    pub stacktrace_id: u32,
    pub value: i64,
}

pub(super) fn selected_sample_rows(
    batch: &RecordBatch,
    trace_ids: Option<&[Vec<u8>]>,
) -> Result<Vec<SelectedSampleRow>, ProfileError> {
    let partitions = batch.column(0).as_primitive::<UInt64Type>();
    let stacktrace_ids = batch.column(1).as_primitive::<UInt64Type>();
    let values = batch.column(2).as_primitive::<Int64Type>();
    let traces = trace_ids.map(|_| batch.column(3).as_binary::<i32>() as &BinaryArray);
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if let Some(wanted) = trace_ids
            && let Some(traces) = traces
            && (traces.is_null(row)
                || !wanted
                    .iter()
                    .any(|trace| trace.as_slice() == traces.value(row)))
        {
            continue;
        }
        let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
            ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
        })?;
        rows.push(SelectedSampleRow {
            partition: partitions.value(row),
            stacktrace_id,
            value: values.value(row),
        });
    }
    Ok(rows)
}
