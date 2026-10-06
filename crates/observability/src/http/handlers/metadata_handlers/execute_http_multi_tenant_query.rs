use super::{
    HttpQueryError, LokiStreamEncoding, QuerierState, QueryKind, QueryParams, TenantId, Value,
    add_loki_query_stats, execute_http_query_for_tenant, json,
    loki_instant_scalar_or_vector_response, loki_range_vector_response, loki_success_value,
    merge_loki_query_response, reject_signed_vector_function_literal, resolved_range_step,
    scalar_vector_expression_result, time_range, validate_loki_query_range_resolution,
    validate_loki_range_query_range_limit,
};
use crate::{LogqlExpr, execute_http_query_for_tenant_inner, parse_logql_expr};

pub(crate) async fn execute_http_multi_tenant_query(
    state: &QuerierState,
    tenants: &[TenantId],
    params: &QueryParams,
    kind: QueryKind,
    encoding: LokiStreamEncoding,
) -> Result<Value, HttpQueryError> {
    reject_signed_vector_function_literal(&params.query)?;
    if parse_logql_expr(&params.query)
        .is_ok_and(|expression| federated_metric_expression(&expression))
    {
        let state = state.with_federated_metric_tenants(tenants);
        if let Some(tenant) = tenants.first() {
            // One evaluator and cap for all tenants, without a cache keyed by
            // only the first tenant's identity.
            return execute_http_query_for_tenant_inner(&state, tenant, params, kind, encoding)
                .await;
        }
    }
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let time_range = time_range(params, kind)?;
        // A scalar expression reads no data, but the window cap still applies,
        // and it applies per tenant: `Loki` takes the smallest of the limits
        // the named tenants carry, so every one of them is checked.
        for tenant in tenants {
            validate_loki_range_query_range_limit(
                &state.with_tenant_limits(tenant),
                kind,
                time_range,
            )?;
        }
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
        let response = execute_http_query_for_tenant(state, tenant, params, kind, encoding).await?;
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

fn federated_metric_expression(expression: &LogqlExpr) -> bool {
    match expression {
        LogqlExpr::Metric { query, .. } => crate::metric_query_uses_approx_topk(query),
        LogqlExpr::Variants { .. }
        | LogqlExpr::Selection {
            approximate: true, ..
        } => true,
        LogqlExpr::Aggregation { expr, .. }
        | LogqlExpr::Sort { expr, .. }
        | LogqlExpr::Selection { expr, .. }
        | LogqlExpr::LabelReplace { expr, .. }
        | LogqlExpr::LabelJoin { expr, .. }
        | LogqlExpr::Vector(expr) => federated_metric_expression(expr),
        LogqlExpr::Arithmetic { left, right, .. }
        | LogqlExpr::Comparison { left, right, .. }
        | LogqlExpr::Set { left, right, .. } => {
            federated_metric_expression(left) || federated_metric_expression(right)
        }
        LogqlExpr::Stream { .. } | LogqlExpr::Scalar(_) => false,
    }
}
