use super::{
    HttpQueryError, QuerierState, QueryKind, SortVectorExpression, TimeRange, Value,
    execute_http_metric_expression_query, sort_loki_vector_result,
};

pub(crate) async fn execute_http_sort_vector_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    sort: SortVectorExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let mut value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &sort.query,
        query_text,
    ))
    .await?;
    sort_loki_vector_result(&mut value, sort.descending);
    Ok(value)
}
