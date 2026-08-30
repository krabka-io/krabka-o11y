use super::{
    DataFusionError, DfResult, Expr, LogicalPlan, UserDefinedLogicalNodeCore, fmt};

/// Logical node: instant-vector selection over a step grid.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct InstantManipulate {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub lookback_delta_ms: i64,
    pub time_index: String,
    pub field_column: String,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for InstantManipulate {
    fn name(&self) -> &'static str {
        "InstantManipulate"
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
            "PromInstantManipulate: start_ms={}, end_ms={}, step_ms={}, lookback_delta_ms={}",
            self.start_ms, self.end_ms, self.step_ms, self.lookback_delta_ms
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "InstantManipulate expects no expressions and one input".to_string(),
            ));
        }
        Ok(Self {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            step_ms: self.step_ms,
            lookback_delta_ms: self.lookback_delta_ms,
            time_index: self.time_index.clone(),
            field_column: self.field_column.clone(),
            input: inputs.swap_remove(0),
        })
    }
}
