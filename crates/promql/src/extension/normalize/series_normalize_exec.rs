use super::*;

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
                    "SeriesNormalize time column `{}` must be Int64",
                    self.time_index
                ))
            })?;
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

        let take_indices = UInt32Array::from_iter_values(
            rows.iter()
                .map(|&(row, _)| u32::try_from(row))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        );
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (index, column) in batch.columns().iter().enumerate() {
            if index == time_column_index {
                columns.push(
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|&(_, ts)| ts)))
                        as ArrayRef,
                );
            } else {
                columns.push(take(column.as_ref(), &take_indices, None)?);
            }
        }

        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
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
                "SeriesNormalizeExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.offset_ms,
            self.time_index.clone(),
            self.need_filter_out_nan,
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
            offset_ms: self.offset_ms,
            time_index: self.time_index.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: Arc::clone(&self.input),
            properties: Arc::clone(&self.properties),
        };
        let stream = input.map(move |batch| batch.and_then(|batch| this.normalize_batch(&batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
