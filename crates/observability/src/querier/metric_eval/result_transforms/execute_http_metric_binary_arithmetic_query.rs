use super::{
    HttpQueryError, MetricBinaryArithmetic, QuerierState, QueryKind, TimeRange, Value,
    apply_metric_binary_arithmetic_to_loki_result, execute_http_metric_query,
};

pub(crate) async fn execute_http_metric_binary_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricBinaryArithmetic,
) -> Result<Value, HttpQueryError> {
    // Both operands are whole queries over the same window and neither reads
    // the other, so awaiting the left one to completion before starting the
    // right one put two full cold scans end to end for every `a / b` panel.
    // Boxed because holding two query futures at once is what makes this one
    // large, and the handlers above inherit whatever size it has.
    let left = Box::pin(execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        arithmetic.left.clone(),
    ));
    let right = Box::pin(execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        arithmetic.right,
    ));
    let (mut left, right) = futures_util::future::try_join(left, right).await?;
    apply_metric_binary_arithmetic_to_loki_result(
        &mut left,
        &right,
        arithmetic.op,
        arithmetic.matching.as_ref(),
    );
    Ok(left)
}
