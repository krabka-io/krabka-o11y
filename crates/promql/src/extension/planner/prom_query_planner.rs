use super::{
    Arc, DefaultPhysicalPlanner, DfResult, ExecutionPlan, LogicalPlan, PhysicalPlanner,
    PromExtensionPlanner, QueryPlanner, Session, async_trait,
};

/// Query planner that adds the custom `PromQL` operator nodes to the default
/// physical planner.
#[derive(Debug, Default)]
pub(crate) struct PromQueryPlanner;

#[async_trait]
impl QueryPlanner for PromQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(PromExtensionPlanner)]);
        physical_planner
            .create_physical_plan(logical_plan, session)
            .await
    }
}
