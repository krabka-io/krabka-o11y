use super::*;

/// Physical node that folds samples into per-eval-step range windows.
#[derive(Debug)]
pub struct RangeManipulateExec {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) interval_ms: i64,
    pub(crate) range_ms: i64,
    pub(crate) time_index: String,
    pub(crate) field_column: String,
    pub(crate) output_schema: SchemaRef,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: Arc<PlanProperties>,
}

impl RangeManipulateExec {
    #[must_use]
    pub fn new(
        start_ms: i64,
        end_ms: i64,
        interval_ms: i64,
        range_ms: i64,
        time_index: String,
        field_column: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let output_schema =
            build_extended_range_schema(&input.schema(), &time_index, &field_column);
        // RangeManipulate rewrites the schema (it drops the scalar time/value
        // columns and appends the windowed RangeArray columns), so the
        // input's `PlanProperties` schema is stale. Build fresh properties keyed
        // on the *output* schema while preserving the input's partitioning,
        // emission, and boundedness so the framework's schema check passes.
        let input_properties = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            input_properties.output_partitioning().clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Self {
            start_ms,
            end_ms,
            interval_ms,
            range_ms,
            time_index,
            field_column,
            output_schema,
            input,
            properties,
        }
    }

    /// Computes the half-open backing-array window `[lo, hi)` for each eval step.
    ///
    /// For eval step `t` on the grid, the window holds the sample rows whose
    /// timestamp falls in `(t-range, t]`. This method returns
    /// `(eval_timestamps, ranges)`, where `ranges[i] == (offset, len)` indexes
    /// the sorted input rows for eval step `eval_timestamps[i]`.
    pub(crate) fn windows(&self, timestamps: &Int64Array) -> DfResult<StepWindows> {
        if self.interval_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "interval_ms must be positive, got {}",
                self.interval_ms
            )));
        }

        let mut eval_timestamps = Vec::new();
        let mut ranges = Vec::new();
        // The samples are time-sorted, so both window edges advance monotonically
        // as the grid steps forward.
        let mut lo = 0_usize;
        let mut hi = 0_usize;
        let mut grid_ts = self.start_ms;
        while grid_ts <= self.end_ms {
            let lower_bound = grid_ts.checked_sub(self.range_ms).ok_or_else(|| {
                DataFusionError::Execution("range lower-bound underflow".to_string())
            })?;
            // Left edge is open: exclude samples with ts <= grid_ts - range.
            while lo < timestamps.len() && timestamps.value(lo) <= lower_bound {
                lo += 1;
            }
            // Right edge is closed: include samples with ts <= grid_ts.
            if hi < lo {
                hi = lo;
            }
            while hi < timestamps.len() && timestamps.value(hi) <= grid_ts {
                hi += 1;
            }

            let offset =
                u32::try_from(lo).map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let len = u32::try_from(hi - lo)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            eval_timestamps.push(grid_ts);
            ranges.push((offset, len));

            grid_ts = grid_ts
                .checked_add(self.interval_ms)
                .ok_or_else(|| DataFusionError::Execution("grid timestamp overflow".to_string()))?;
        }

        Ok((eval_timestamps, ranges))
    }

    pub(crate) fn manipulate_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
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
                    "RangeManipulate time column `{}` must be Int64",
                    self.time_index
                ))
            })?;
        let value_column_index = batch
            .schema()
            .index_of(&self.field_column)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let values = batch
            .column(value_column_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "RangeManipulate field column `{}` must be Float64",
                    self.field_column
                ))
            })?;

        // An empty input series has no labels to project and no samples to
        // window, so it contributes no output rows.
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        let (eval_timestamps, ranges) = self.windows(timestamps)?;

        let timestamps_values = Arc::new(timestamps.clone()) as ArrayRef;
        let values_values = Arc::new(values.clone()) as ArrayRef;
        let timestamp_range = RangeArray::from_ranges(timestamps_values, ranges.iter().copied())
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let value_range = RangeArray::from_ranges(values_values, ranges.iter().copied())
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let timestamp_range = timestamp_range
            .into_dict_array()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let value_range = value_range
            .into_dict_array()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;

        // Label values repeat per eval step: take row 0 of each label column for
        // every output row (one series per batch, so all rows share labels).
        let take_indices =
            UInt32Array::from_iter_values(std::iter::repeat_n(0_u32, eval_timestamps.len()));

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.output_schema.fields().len());
        for (index, column) in batch.columns().iter().enumerate() {
            if index == time_column_index || index == value_column_index {
                continue;
            }
            columns.push(take(column.as_ref(), &take_indices, None)?);
        }

        columns.push(Arc::new(Int64Array::from_iter_values(
            eval_timestamps.iter().copied(),
        )) as ArrayRef);
        columns.push(Arc::new(timestamp_range) as ArrayRef);
        columns.push(Arc::new(value_range) as ArrayRef);

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }
}

impl DisplayAs for RangeManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromRangeManipulateExec: start_ms={}, end_ms={}, interval_ms={}, range_ms={}",
            self.start_ms, self.end_ms, self.interval_ms, self.range_ms
        )
    }
}

impl ExecutionPlan for RangeManipulateExec {
    fn name(&self) -> &'static str {
        "RangeManipulateExec"
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
                "RangeManipulateExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
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
        let schema = Arc::clone(&self.output_schema);
        let this = Self {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            interval_ms: self.interval_ms,
            range_ms: self.range_ms,
            time_index: self.time_index.clone(),
            field_column: self.field_column.clone(),
            output_schema: Arc::clone(&self.output_schema),
            input: Arc::clone(&self.input),
            properties: Arc::clone(&self.properties),
        };
        let stream = input.map(move |batch| batch.and_then(|batch| this.manipulate_batch(&batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
