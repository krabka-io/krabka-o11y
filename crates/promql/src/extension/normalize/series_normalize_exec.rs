use super::{
    Arc, DataFusionError, DfResult, DisplayAs, DisplayFormatType, ExecutionPlan, Float64Array,
    PlanProperties, RecordBatch, SendableRecordBatchStream, TaskContext, fmt,
};
use crate::extension::{
    RowSelection, TimeColumn, map_batches, only_child, take_rows_with_timestamps,
};

/// Physical node that normalizes single-series batches.
#[derive(Debug)]
pub struct SeriesNormalizeExec {
    pub(crate) offset_ms: i64,
    pub(crate) time_index: String,
    pub(crate) need_filter_out_nan: bool,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: Arc<PlanProperties>,
}

impl SeriesNormalizeExec {
    #[must_use]
    pub fn new(
        offset_ms: i64,
        time_index: String,
        need_filter_out_nan: bool,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            offset_ms,
            time_index,
            need_filter_out_nan,
            input,
            properties,
        }
    }

    pub(crate) fn normalize_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        let (time_column_index, timestamps) = TimeColumn {
            node: "SeriesNormalize",
            name: &self.time_index,
        }
        .find(batch)?;
        let values = batch
            .column_by_name("value")
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>());

        let mut rows = (0..batch.num_rows())
            .filter(|&row| {
                !self.need_filter_out_nan
                    || values.is_none_or(|value_array| !value_array.value(row).is_nan())
            })
            .map(|row| {
                timestamps
                    .value(row)
                    .checked_add(self.offset_ms)
                    .map(|ts| (row, ts))
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "timestamp offset overflow at row {row}"
                        ))
                    })
            })
            .collect::<DfResult<Vec<_>>>()?;
        rows.sort_by_key(|&(row, ts)| (ts, row));

        let take_rows = rows
            .iter()
            .map(|&(row, _)| u32::try_from(row))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        take_rows_with_timestamps(
            batch,
            time_column_index,
            RowSelection {
                rows: take_rows,
                timestamps: rows.iter().map(|&(_, ts)| ts).collect(),
            },
        )
    }
}

impl DisplayAs for SeriesNormalizeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromSeriesNormalizeExec: time={}, offset_ms={}, filter_nan={}",
            self.time_index, self.offset_ms, self.need_filter_out_nan
        )
    }
}

impl ExecutionPlan for SeriesNormalizeExec {
    fn name(&self) -> &'static str {
        "SeriesNormalizeExec"
    }

    single_input_exec_plumbing!();

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let input = only_child(children, self.name())?;
        Ok(Arc::new(Self::new(
            self.offset_ms,
            self.time_index.clone(),
            self.need_filter_out_nan,
            input,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let this = Self {
            offset_ms: self.offset_ms,
            time_index: self.time_index.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: Arc::clone(&self.input),
            properties: Arc::clone(&self.properties),
        };
        Ok(map_batches(input, schema, move |batch| {
            this.normalize_batch(batch)
        }))
    }
}
