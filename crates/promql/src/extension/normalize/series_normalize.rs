use super::{LogicalPlan, UserDefinedLogicalNodeCore, Expr, fmt, DfResult, DataFusionError};

/// Logical node that normalizes each single-series batch.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesNormalize {
    pub offset_ms: i64,
    pub time_index: String,
    pub need_filter_out_nan: bool,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesNormalize {
    fn name(&self) -> &'static str {
        "SeriesNormalize"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromSeriesNormalize: time={}, offset_ms={}, filter_nan={}",
            self.time_index, self.offset_ms, self.need_filter_out_nan
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "SeriesNormalize expects no expressions and one input".to_string(),
            ));
        }
        Ok(Self {
            offset_ms: self.offset_ms,
            time_index: self.time_index.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: inputs.swap_remove(0),
        })
    }
}
