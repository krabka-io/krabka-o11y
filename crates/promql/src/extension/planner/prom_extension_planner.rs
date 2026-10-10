use super::{
    Arc, DfResult, ExecutionPlan, ExtensionPlanner, InstantManipulate, InstantManipulateExec,
    LogicalPlan, PhysicalPlanner, PhysicalPlanningContext, RangeManipulate, RangeManipulateExec,
    SeriesDivide, SeriesDivideExec, SeriesNormalize, SeriesNormalizeExec, Session,
    UserDefinedLogicalNode, async_trait, single_input,
};

/// Maps the custom `PromQL` logical nodes to their physical `Exec` nodes.
#[derive(Debug, Default)]
pub struct PromExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for PromExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        if let Some(divide) = any.downcast_ref::<SeriesDivide>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(SeriesDivideExec::new(
                divide.tag_columns.clone(),
                input,
            ))));
        }
        if let Some(normalize) = any.downcast_ref::<SeriesNormalize>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(SeriesNormalizeExec::new(
                normalize.settings.clone(),
                input,
            ))));
        }
        if let Some(instant) = any.downcast_ref::<InstantManipulate>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(InstantManipulateExec::new(
                instant.settings.clone(),
                input,
            ))));
        }
        if let Some(range) = any.downcast_ref::<RangeManipulate>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(RangeManipulateExec::new(
                range.settings.clone(),
                input,
            ))));
        }
        Ok(None)
    }
}
