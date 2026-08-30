use super::*;

pub(crate) async fn execute_http_metric_binary_set_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricBinarySet,
) -> Result<Value, HttpQueryError> {
    let mut left =
        execute_http_metric_query(state, tenant, time_range, step, kind, set.left.clone()).await?;
    let right = execute_http_metric_query(state, tenant, time_range, step, kind, set.right).await?;
    apply_metric_binary_set_to_loki_result(&mut left, &right, set.op, set.matching.as_ref());
    Ok(left)
}
