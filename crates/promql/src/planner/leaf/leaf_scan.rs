use super::{
    Arc, LogicalPlan, LogicalPlanBuilder, MemTable, PromqlError, RecordBatch, Result, Schema,
    provider_as_source,
};

/// Wraps one leaf batch in a `TableScan` that no catalog knows about.
///
/// The scan's [`TableSource`](datafusion::logical_expr::TableSource) carries the
/// [`MemTable`] itself, so `DataFusion`'s physical planner resolves it straight
/// from the plan. Nothing is registered on the [`SessionContext`], which is what
/// lets every `PromQL` plan share one context: a registration under a fixed
/// table name would collide between the steps of a range query and between the
/// nested leaves of a single expression, since an aggregate over a rate builds
/// more than one leaf. `name` therefore only ever appears in `EXPLAIN` output.
///
/// [`SessionContext`]: datafusion::prelude::SessionContext
///
/// # Errors
///
/// Returns [`PromqlError::Exec`] if the batch does not match `schema` or the
/// scan cannot be built.
pub(crate) fn leaf_scan(
    name: &str,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> Result<LogicalPlan> {
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)
        .and_then(LogicalPlanBuilder::build)
        .map_err(|error| PromqlError::Exec(error.to_string()))
}
