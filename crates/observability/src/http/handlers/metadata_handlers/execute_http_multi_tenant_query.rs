use super::*;

pub(crate) async fn execute_http_multi_tenant_query(
    state: &QuerierState,
    tenants: &[String],
    params: &QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    reject_signed_vector_function_literal(&params.query)?;
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let time_range = time_range(params, kind)?;
        validate_loki_range_query_range_limit(kind, time_range)?;
        validate_loki_query_range_resolution(params, kind, time_range)?;
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => loki_range_vector_response(
                time_range,
                resolved_range_step(params.step, time_range)?,
                result,
            ),
        };
        return Ok(add_loki_query_stats(value));
    }

    let mut merged = None;
    for tenant in tenants {
        let response = execute_http_query_for_tenant(state, tenant, params, kind).await?;
        match &mut merged {
            Some(merged) => merge_loki_query_response(merged, &response),
            None => merged = Some(response),
        }
    }
    Ok(merged.unwrap_or_else(|| {
        add_loki_query_stats(loki_success_value(json!({
            "resultType": "streams",
            "result": []
        })))
    }))
}
