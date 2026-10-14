use arrow::array::{BinaryArray, Float64Array, StringArray};
use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JobValue {
    pub(crate) job: String,
    pub(crate) value: f64,
}

/// Executes `plan` in `ctx` and returns each output row's `job` label with its
/// `value_column` float, in output order.
pub(crate) async fn job_values(
    ctx: &SessionContext,
    plan: LogicalPlan,
    value_column: &str,
) -> Vec<JobValue> {
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut got = Vec::new();
    for batch in &batches {
        let job = batch.column_by_name("job").unwrap();
        let value = batch
            .column_by_name(value_column)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let job = job
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|values| values.value(row).to_string())
                .or_else(|| {
                    job.as_any()
                        .downcast_ref::<BinaryArray>()
                        .map(|values| String::from_utf8(values.value(row).to_vec()).unwrap())
                })
                .unwrap();
            got.push(JobValue {
                job,
                value: value.value(row),
            });
        }
    }
    got
}
