use super::{async_trait, ExtensionPlanner, PhysicalPlanner, UserDefinedLogicalNode, LogicalPlan, Arc, ExecutionPlan, Session, PhysicalPlanningContext, DfResult, SeriesDivide, single_input, SeriesDivideExec, SeriesNormalize, SeriesNormalizeExec, InstantManipulate, InstantManipulateExec, RangeManipulate, RangeManipulateExec};

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
                normalize.offset_ms,
                normalize.time_index.clone(),
                normalize.need_filter_out_nan,
                input,
            ))));
        }
        if let Some(instant) = any.downcast_ref::<InstantManipulate>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(InstantManipulateExec::new(
                instant.start_ms,
                instant.end_ms,
                instant.step_ms,
                instant.lookback_delta_ms,
                instant.time_index.clone(),
                instant.field_column.clone(),
                input,
            ))));
        }
        if let Some(range) = any.downcast_ref::<RangeManipulate>() {
            let input = single_input(physical_inputs)?;
            return Ok(Some(Arc::new(RangeManipulateExec::new(
                range.start_ms,
                range.end_ms,
                range.interval_ms,
                range.range_ms,
                range.time_index.clone(),
                range.field_column.clone(),
                input,
            ))));
        }
        Ok(None)
    }
}
