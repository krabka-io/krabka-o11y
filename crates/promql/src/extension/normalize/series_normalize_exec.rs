use std::{fmt, sync::Arc};

use arrow::{array::Float64Array, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};

use super::{NanSamples, SeriesNormalizeSettings};
use crate::extension::{RowSelection, TimeColumn, take_rows_with_timestamps};

/// Physical node that normalizes single-series batches.
#[derive(Debug, Clone)]
pub struct SeriesNormalizeExec {
    pub(crate) settings: SeriesNormalizeSettings,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: Arc<PlanProperties>,
}

impl SeriesNormalizeExec {
    #[must_use]
    pub fn new(settings: SeriesNormalizeSettings, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            settings,
            input,
            properties,
        }
    }

    pub(crate) fn normalize_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        let (time_column_index, timestamps) = TimeColumn {
            node: "SeriesNormalize",
            name: &self.settings.time_index,
        }
        .find(batch)?;
        let values = batch
            .column_by_name("value")
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>());

        let mut rows = (0..batch.num_rows())
            .filter(|&row| {
                self.settings.nan_samples == NanSamples::Keep
                    || values.is_none_or(|value_array| !value_array.value(row).is_nan())
            })
            .map(|row| {
                timestamps
                    .value(row)
                    .checked_add(self.settings.offset_ms)
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
            self.settings.time_index,
            self.settings.offset_ms,
            self.settings.nan_samples == NanSamples::Drop
        )
    }
}

impl ExecutionPlan for SeriesNormalizeExec {
    fn name(&self) -> &'static str {
        "SeriesNormalizeExec"
    }

    single_input_exec_plumbing!();

    settings_batch_exec_methods!(normalize_batch);
}
