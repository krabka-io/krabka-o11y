use super::{Arc, ExecutionPlan, DfResult};

pub(crate) fn single_input(physical_inputs: &[Arc<dyn ExecutionPlan>]) -> DfResult<Arc<dyn ExecutionPlan>> {
    match physical_inputs {
        [input] => Ok(Arc::clone(input)),
        _ => Err(datafusion::error::DataFusionError::Plan(
            "PromQL operator node expects exactly one input".to_string(),
        )),
    }
}
