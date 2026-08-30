use super::*;

pub(crate) async fn execute_http_metric_binary_operand(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    operand: &str,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    if let Some(label_replace) = parse_label_replace_expression(operand) {
        let mut value = execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            &label_replace.query,
            query_text,
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            query_text,
        )?;
        return Ok(value);
    }
    if scalar_vector_query_is_vector(operand) {
        return execute_http_scalar_vector_expression_result(
            operand, time_range, step, kind, query_text,
        );
    }

    let query = parse_metric_query(operand).map_err(|source| HttpQueryError::LokiParse {
        query: query_text.to_string(),
        source,
    })?;
    execute_http_metric_query(state, tenant, time_range, step, kind, query).await
}
