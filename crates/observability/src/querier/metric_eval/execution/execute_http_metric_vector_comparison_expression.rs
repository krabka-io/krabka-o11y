use super::{
    HttpQueryError, MetricVectorComparisonExpression, QuerierState, QueryKind, TimeRange, Value,
    apply_metric_binary_comparison_to_loki_result, execute_http_metric_expression_query,
    execute_http_scalar_vector_expression_result, merge_loki_query_stats,
    retain_metric_binary_on_labels,
};

pub(crate) async fn execute_http_metric_vector_comparison_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricVectorComparisonExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &comparison.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &comparison.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if comparison.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &metric_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &vector_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        Ok(value)
    }
}
