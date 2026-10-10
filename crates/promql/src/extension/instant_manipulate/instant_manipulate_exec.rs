use std::{fmt, sync::Arc};

use arrow::{array::Float64Array, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};

use super::InstantManipulateSettings;
use crate::extension::{RowSelection, TimeColumn, take_rows_with_timestamps};

/// Physical node that emits one selected sample per valid grid step.
#[derive(Debug, Clone)]
pub struct InstantManipulateExec {
    pub(crate) settings: InstantManipulateSettings,
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: Arc<PlanProperties>,
}

impl InstantManipulateExec {
    #[must_use]
    pub fn new(settings: InstantManipulateSettings, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            settings,
            input,
            properties,
        }
    }

    pub(crate) fn manipulate_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        if self.settings.step_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "step_ms must be positive, got {}",
                self.settings.step_ms
            )));
        }
        let (time_column_index, timestamps) = TimeColumn {
            node: "InstantManipulate",
            name: &self.settings.time_index,
        }
        .find(batch)?;
        let values = batch
            .column_by_name(&self.settings.field_column)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "InstantManipulate field column `{}` must be Float64",
                    self.settings.field_column
                ))
            })?;

        let mut selected_rows = Vec::new();
        let mut output_timestamps = Vec::new();
        let mut sample_cursor = 0_usize;
        let mut grid_ts = self.settings.start_ms;
        while grid_ts <= self.settings.end_ms {
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
                if grid_ts - sample_ts < self.settings.lookback_delta_ms
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
                .checked_add(self.settings.step_ms)
                .ok_or_else(|| DataFusionError::Execution("grid timestamp overflow".to_string()))?;
        }

        take_rows_with_timestamps(
            batch,
            time_column_index,
            RowSelection {
                rows: selected_rows,
                timestamps: output_timestamps,
            },
        )
    }
}

impl DisplayAs for InstantManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromInstantManipulateExec: start_ms={}, end_ms={}, step_ms={}, lookback_delta_ms={}",
            self.settings.start_ms,
            self.settings.end_ms,
            self.settings.step_ms,
            self.settings.lookback_delta_ms
        )
    }
}

impl ExecutionPlan for InstantManipulateExec {
    fn name(&self) -> &'static str {
        "InstantManipulateExec"
    }

    single_input_exec_plumbing!();

    settings_batch_exec_methods!(manipulate_batch);
}
