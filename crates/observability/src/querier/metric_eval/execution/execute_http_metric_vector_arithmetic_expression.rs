use super::{
    HttpQueryError, MetricVectorArithmeticExpression, QuerierState, QueryKind, TimeRange, Value,
    apply_metric_binary_arithmetic_to_loki_result, execute_http_metric_expression_query,
    execute_http_scalar_vector_expression_result, merge_loki_query_stats,
    retain_metric_binary_on_labels,
};

pub(crate) async fn execute_http_metric_vector_arithmetic_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricVectorArithmeticExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &arithmetic.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &arithmetic.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if arithmetic.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &metric_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &vector_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        Ok(value)
    }
}
