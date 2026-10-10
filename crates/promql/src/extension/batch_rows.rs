//! Batch plumbing shared by the per-batch `Prom*Exec` operators.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, UInt32Array},
    compute::take,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;

/// An operator's `Int64` time column, by name.
pub(crate) struct TimeColumn<'a> {
    /// The operator that reads the column, as its errors name it.
    pub(crate) node: &'static str,
    pub(crate) name: &'a str,
}

impl TimeColumn<'_> {
    /// Finds the column in `batch`, and returns its index and timestamps.
    pub(crate) fn find<'b>(&self, batch: &'b RecordBatch) -> DfResult<(usize, &'b Int64Array)> {
        let index = batch
            .schema()
            .index_of(self.name)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let timestamps = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{} time column `{}` must be Int64",
                    self.node, self.name
                ))
            })?;
        Ok((index, timestamps))
    }
}

/// The rows an operator keeps from a batch, in output order, with the time
/// each kept row carries.
pub(crate) struct RowSelection {
    /// Input row indices.
    pub(crate) rows: Vec<u32>,
    /// One output timestamp per entry of `rows`.
    pub(crate) timestamps: Vec<i64>,
}

/// Takes the `selection` rows of `batch` in order, replacing its time column
/// (at `time_column_index`) with the selection's timestamps.
pub(crate) fn take_rows_with_timestamps(
    batch: &RecordBatch,
    time_column_index: usize,
    selection: RowSelection,
) -> DfResult<RecordBatch> {
    let take_indices = UInt32Array::from_iter_values(selection.rows);
    let mut timestamps = Some(selection.timestamps);
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, column) in batch.columns().iter().enumerate() {
        if index == time_column_index
            && let Some(timestamps) = timestamps.take()
        {
            columns.push(Arc::new(Int64Array::from(timestamps)) as ArrayRef);
        } else {
            columns.push(take(column.as_ref(), &take_indices, None)?);
        }
    }

    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

/// Maps each batch of `input` through `transform` into a stream of `schema`.
pub(crate) fn map_batches(
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    mut transform: impl FnMut(&RecordBatch) -> DfResult<RecordBatch> + Send + 'static,
) -> SendableRecordBatchStream {
    let stream = input.map(move |batch| batch.and_then(|batch| transform(&batch)));
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
