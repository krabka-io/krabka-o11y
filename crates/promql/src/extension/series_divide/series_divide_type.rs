use super::{DfResult, Expr, LogicalPlan, UserDefinedLogicalNodeCore, fmt};
use crate::extension::only_logical_input;

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

    pass_through_logical_node_plumbing!();

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PromSeriesDivide: tags={:?}", self.tag_columns)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = only_logical_input(&exprs, inputs, "SeriesDivide")?;
        Ok(Self {
            tag_columns: self.tag_columns.clone(),
            input,
        })
    }
}
