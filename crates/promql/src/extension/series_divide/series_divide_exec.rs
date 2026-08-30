use super::{Arc, DataFusionError, DfResult, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatch, RecordBatchStreamAdapter, SendableRecordBatchStream, StreamExt, TaskContext, UserDefinedLogicalNodeCore, array_value_to_string, fmt};

/// Physical node that emits one batch per contiguous series run.
#[derive(Debug)]
pub struct SeriesDivideExec {
    pub(crate) tag_columns: Vec<String>,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: Arc<PlanProperties>,
}

impl SeriesDivideExec {
    #[must_use]
    pub fn new(tag_columns: Vec<String>, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            tag_columns,
            input,
            properties,
        }
    }

    pub(crate) fn split_batch(tag_columns: &[String], batch: RecordBatch) -> DfResult<Vec<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(vec![batch]);
        }
        let mut boundaries = vec![0_usize];
        for row in 1..batch.num_rows() {
            if Self::series_changed(tag_columns, &batch, row - 1, row)? {
                boundaries.push(row);
            }
        }
        boundaries.push(batch.num_rows());

        Ok(boundaries
            .windows(2)
            .map(|window| {
                let start = window[0];
                let len = window[1] - start;
                batch.slice(start, len)
            })
            .collect())
    }

    pub(crate) fn series_changed(
        tag_columns: &[String],
        batch: &RecordBatch,
        left: usize,
        right: usize,
    ) -> DfResult<bool> {
        for column_name in tag_columns {
            let column = batch.column_by_name(column_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "SeriesDivide tag column `{column_name}` not found"
                ))
            })?;
            // A NULL label entry (ABSENT label) must not compare equal to a
            // present-but-empty (`""`) entry: `array_value_to_string` renders
            // both as `""`, so compare nullness first. Two series that differ
            // only in whether a label is absent vs present-empty are distinct.
            let left_null = column.is_null(left);
            let right_null = column.is_null(right);
            if left_null != right_null {
                return Ok(true);
            }
            if left_null {
                // Both NULL on this column: identical here, check the next.
                continue;
            }
            let left_value = array_value_to_string(column.as_ref(), left)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let right_value = array_value_to_string(column.as_ref(), right)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            if left_value != right_value {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl DisplayAs for SeriesDivideExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PromSeriesDivideExec: tags={:?}", self.tag_columns)
    }
}

impl ExecutionPlan for SeriesDivideExec {
    fn name(&self) -> &'static str {
        "SeriesDivideExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "SeriesDivideExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.tag_columns.clone(),
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
        let tag_columns = self.tag_columns.clone();
        let stream = input
            .map(move |batch| match batch {
                Ok(batch) => match Self::split_batch(&tag_columns, batch) {
                    Ok(batches) => futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
                    Err(error) => futures::stream::iter([Err(error)]).boxed(),
                },
                Err(error) => futures::stream::iter([Err(error)]).boxed(),
            })
            .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
