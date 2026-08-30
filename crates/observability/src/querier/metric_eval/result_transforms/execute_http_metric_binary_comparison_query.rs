use super::{
    HttpQueryError, MetricBinaryComparison, QuerierState, QueryKind, TimeRange, Value,
    apply_metric_binary_comparison_to_loki_result, execute_http_metric_query,
};

pub(crate) async fn execute_http_metric_binary_comparison_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricBinaryComparison,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        comparison.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, comparison.right).await?;
    apply_metric_binary_comparison_to_loki_result(
        &mut left,
        &right,
        comparison.op,
        comparison.bool_modifier,
        comparison.matching.as_ref(),
    );
    Ok(left)
}
