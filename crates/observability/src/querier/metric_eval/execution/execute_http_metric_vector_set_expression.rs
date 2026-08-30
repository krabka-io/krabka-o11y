use super::{
    HttpQueryError, MetricVectorSetExpression, QuerierState, QueryKind, TimeRange, Value,
    apply_metric_binary_set_to_loki_result, execute_http_metric_expression_query,
    execute_http_scalar_vector_expression_result, merge_loki_query_stats,
    normalize_loki_vector_sample_timestamps_to_seconds,
};

pub(crate) async fn execute_http_metric_vector_set_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricVectorSetExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &set.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &set.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if set.vector_on_left {
        let mut value = vector_value;
        if matches!(kind, QueryKind::Instant) {
            normalize_loki_vector_sample_timestamps_to_seconds(&mut value);
        }
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &metric_value,
            set.op,
            set.matching.as_ref(),
        );
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &vector_value,
            set.op,
            set.matching.as_ref(),
        );
        Ok(value)
    }
}
