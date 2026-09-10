use super::{
    HttpQueryError, MetricBinarySet, QuerierState, QueryKind, TimeRange, Value,
    apply_metric_binary_set_to_loki_result, execute_http_metric_query,
};

pub(crate) async fn execute_http_metric_binary_set_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricBinarySet,
) -> Result<Value, HttpQueryError> {
    let left = Box::pin(execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        set.left.clone(),
    ));
    let right = Box::pin(execute_http_metric_query(
        state, tenant, time_range, step, kind, set.right,
    ));
    let (mut left, right) = futures_util::future::try_join(left, right).await?;
    apply_metric_binary_set_to_loki_result(&mut left, &right, set.op, set.matching.as_ref());
    Ok(left)
}
