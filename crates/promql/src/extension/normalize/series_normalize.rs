use std::fmt;

use datafusion::{
    common::Result as DfResult,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
};

use crate::extension::only_logical_input;

/// Whether series normalization keeps or drops samples whose value is NaN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum NanSamples {
    /// NaN-valued samples pass through.
    Keep,
    /// NaN-valued samples are filtered out.
    Drop,
}

/// How each single series is normalized, shared by the logical
/// [`SeriesNormalize`] node and its physical `SeriesNormalizeExec`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesNormalizeSettings {
    /// The shift added to every sample timestamp, in milliseconds.
    pub offset_ms: i64,
    /// The `Int64` sample-timestamp column.
    pub time_index: String,
    /// Whether NaN-valued samples survive.
    pub nan_samples: NanSamples,
}

/// Logical node that normalizes each single-series batch.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesNormalize {
    pub settings: SeriesNormalizeSettings,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesNormalize {
    fn name(&self) -> &'static str {
        "SeriesNormalize"
    }

    pass_through_logical_node_plumbing!();

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromSeriesNormalize: time={}, offset_ms={}, filter_nan={}",
            self.settings.time_index,
            self.settings.offset_ms,
            self.settings.nan_samples == NanSamples::Drop
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = only_logical_input(&exprs, inputs, "SeriesNormalize")?;
        Ok(Self {
            settings: self.settings.clone(),
            input,
        })
    }
}
