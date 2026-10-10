use arrow::array::Float64Array;
use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

/// Executes `plan` in `ctx` and returns the first output batch's
/// `value_column` floats.
pub(crate) async fn first_batch_values(
    ctx: &SessionContext,
    plan: LogicalPlan,
    value_column: &str,
) -> Float64Array {
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column_by_name(value_column)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .clone()
}
