use super::{
    Arc, ArrayRef, DataFusionError, DfResult, DisplayAs, DisplayFormatType, ExecutionPlan,
    Float64Array, Int64Array, PlanProperties, RecordBatch, RecordBatchStreamAdapter,
    SendableRecordBatchStream, StreamExt, TaskContext, UInt32Array, fmt, take,
};

/// Physical node that emits one selected sample per valid grid step.
#[derive(Debug)]
pub struct InstantManipulateExec {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) step_ms: i64,
    pub(crate) lookback_delta_ms: i64,
    pub(crate) time_index: String,
    pub(crate) field_column: String,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: Arc<PlanProperties>,
}

impl InstantManipulateExec {
    #[must_use]
    pub fn new(
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
        time_index: String,
        field_column: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            start_ms,
            end_ms,
            step_ms,
            lookback_delta_ms,
            time_index,
            field_column,
            input,
            properties,
        }
    }

    pub(crate) fn manipulate_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        if self.step_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "step_ms must be positive, got {}",
                self.step_ms
            )));
        }
        let time_column_index = batch
            .schema()
            .index_of(&self.time_index)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let timestamps = batch
            .column(time_column_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "InstantManipulate time column `{}` must be Int64",
                    self.time_index
                ))
            })?;
        let values = batch
            .column_by_name(&self.field_column)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "InstantManipulate field column `{}` must be Float64",
                    self.field_column
                ))
            })?;

        let mut selected_rows = Vec::new();
        let mut output_timestamps = Vec::new();
        let mut sample_cursor = 0_usize;
        let mut grid_ts = self.start_ms;
        while grid_ts <= self.end_ms {
            while sample_cursor < timestamps.len() && timestamps.value(sample_cursor) <= grid_ts {
                sample_cursor = sample_cursor.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution("sample cursor overflow".to_string())
                })?;
            }
            if let Some(row) = sample_cursor.checked_sub(1) {
                let sample_ts = timestamps.value(row);
                // Drop the selected sample only when it is Prometheus' stale-NaN
                // marker (the series has been terminated); a genuine NaN value is
                // kept as a NaN sample, matching `engine::eval_instant_selector`.
                if grid_ts - sample_ts < self.lookback_delta_ms
                    && !super::super::is_stale_nan(values.value(row))
                {
                    selected_rows.push(
                        u32::try_from(row)
                            .map_err(|error| DataFusionError::Execution(error.to_string()))?,
                    );
                    output_timestamps.push(grid_ts);
                }
            }
            grid_ts = grid_ts
                .checked_add(self.step_ms)
                .ok_or_else(|| DataFusionError::Execution("grid timestamp overflow".to_string()))?;
        }

        let take_indices = UInt32Array::from_iter_values(selected_rows);
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (index, column) in batch.columns().iter().enumerate() {
            if index == time_column_index {
                columns.push(Arc::new(Int64Array::from_iter_values(
                    output_timestamps.iter().copied(),
                )) as ArrayRef);
            } else {
                columns.push(take(column.as_ref(), &take_indices, None)?);
            }
        }

        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }
}

impl DisplayAs for InstantManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromInstantManipulateExec: start_ms={}, end_ms={}, step_ms={}, lookback_delta_ms={}",
            self.start_ms, self.end_ms, self.step_ms, self.lookback_delta_ms
        )
    }
}

impl ExecutionPlan for InstantManipulateExec {
    fn name(&self) -> &'static str {
        "InstantManipulateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "InstantManipulateExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.start_ms,
            self.end_ms,
            self.step_ms,
            self.lookback_delta_ms,
            self.time_index.clone(),
            self.field_column.clone(),
            children.swap_remove(0),
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
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            step_ms: self.step_ms,
            lookback_delta_ms: self.lookback_delta_ms,
            time_index: self.time_index.clone(),
            field_column: self.field_column.clone(),
            input: Arc::clone(&self.input),
            properties: Arc::clone(&self.properties),
        };
        let stream = input.map(move |batch| batch.and_then(|batch| this.manipulate_batch(&batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
