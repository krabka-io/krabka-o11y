use std::sync::Arc;

use datafusion::{
    common::{DataFusionError, Result as DfResult},
    logical_expr::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
};

/// Takes the one child a single-input exec named `exec` is rebuilt over, and
/// rejects any other number of children.
pub(crate) fn only_child(
    mut children: Vec<Arc<dyn ExecutionPlan>>,
    exec: &str,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    if children.len() != 1 {
        return Err(DataFusionError::Plan(format!("{exec} expects one child")));
    }
    Ok(children.swap_remove(0))
}

/// Takes the one input a single-input logical node named `node` is rebuilt
/// over, and rejects any expression or any other number of inputs.
pub(crate) fn only_logical_input(
    exprs: &[Expr],
    mut inputs: Vec<LogicalPlan>,
    node: &str,
) -> DfResult<LogicalPlan> {
    if !exprs.is_empty() || inputs.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "{node} expects no expressions and one input"
        )));
    }
    Ok(inputs.swap_remove(0))
}
