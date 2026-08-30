use super::*;

pub(crate) fn execute_http_scalar_vector_expression_result(
    query: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let vector_result =
        scalar_vector_expression_result(query).ok_or_else(|| HttpQueryError::LokiParse {
            query: query_text.to_string(),
            source: ParseError::Syntax {
                message: "expected vector expression".to_string(),
                position: 0,
            },
        })?;
    let value = match kind {
        QueryKind::Instant => {
            loki_instant_scalar_or_vector_response(time_range.end_ns, vector_result)
        }
        QueryKind::Range => loki_range_vector_response(
            time_range,
            resolved_range_step(step, time_range)?,
            vector_result,
        ),
    };
    Ok(add_loki_query_stats(value))
}
