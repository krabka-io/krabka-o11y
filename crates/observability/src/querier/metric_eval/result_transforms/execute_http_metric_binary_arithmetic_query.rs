use super::*;

pub(crate) async fn execute_http_metric_binary_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricBinaryArithmetic,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        arithmetic.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, arithmetic.right).await?;
    apply_metric_binary_arithmetic_to_loki_result(
        &mut left,
        &right,
        arithmetic.op,
        arithmetic.matching.as_ref(),
    );
    Ok(left)
}
