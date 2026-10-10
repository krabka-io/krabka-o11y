use std::sync::Arc;

use datafusion::{
    common::{DataFusionError, Result as DfResult},
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
