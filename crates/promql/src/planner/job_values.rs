use arrow::array::{BinaryArray, Float64Array};
use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

/// Executes `plan` in `ctx` and returns each output row's `job` label with its
/// `value_column` float, in output order.
pub(crate) async fn job_values(
    ctx: &SessionContext,
    plan: LogicalPlan,
    value_column: &str,
) -> Vec<(String, f64)> {
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut got = Vec::new();
    for batch in &batches {
        let job = batch
            .column_by_name("job")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let value = batch
            .column_by_name(value_column)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            got.push((
                String::from_utf8(job.value(row).to_vec()).unwrap(),
                value.value(row),
            ));
        }
    }
    got
}
