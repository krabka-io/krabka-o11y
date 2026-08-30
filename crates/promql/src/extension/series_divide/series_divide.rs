use super::{DataFusionError, DfResult, ExecutionPlan, Expr, LogicalPlan, UserDefinedLogicalNodeCore, fmt};

/// Logical node: partition the input into per-series batches.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesDivide {
    pub tag_columns: Vec<String>,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesDivide {
    fn name(&self) -> &'static str {
        "SeriesDivide"
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
        write!(f, "PromSeriesDivide: tags={:?}", self.tag_columns)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "SeriesDivide expects no expressions and one input".to_string(),
            ));
        }
        Ok(Self {
            tag_columns: self.tag_columns.clone(),
            input: inputs.swap_remove(0),
        })
    }
}
