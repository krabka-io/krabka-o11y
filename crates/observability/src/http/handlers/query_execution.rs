use std::collections::BTreeMap;

use crate::{
    ComparisonResult, HttpMetricQuery, HttpQueryError, HttpStreamQuery, LabelReplaceArguments,
    Labels, LogqlExpr, LokiStreamEncoding, LokiStreamOptions, MetricComparison,
    MetricVectorMatching, QuerierState, QueryKind, QueryParams, SampleOrder, ScalarArithmetic,
    ScalarComparison, ScalarLiteral, ScalarSide, ScalarVectorExpressionResult, TenantId, TimeRange,
    Value, VectorAggregation, VectorAggregationOp, VectorArithmetic, VectorComparison,
    add_loki_query_stats, apply_label_join_fields, apply_label_replace_to_loki_result,
    apply_metric_binary_arithmetic_to_loki_result, apply_metric_binary_comparison_to_loki_result,
    apply_metric_binary_set_to_loki_result, apply_metric_selection,
    apply_scalar_arithmetic_to_loki_result, apply_scalar_comparison_to_loki_result,
    clamp_query_lookback, current_unix_time_ns, execute_http_metric_query,
    execute_http_stream_query, json, loki_direction, loki_instant_scalar_or_vector_response,
    loki_range_vector_response, merge_loki_query_stats, metric_series_labels, parse_logql_expr,
    populate_loki_query_execution_stats, querier::aggregate::sample_windows::vector_group_labels,
    reject_signed_vector_function_literal, resolved_range_step, retain_metric_binary_on_labels,
    scalar_vector_expression_result, sort_loki_vector_result, strip_outer_parenthesized_expression,
    time_range, validate_loki_query_range_resolution, validate_loki_range_query_range_limit,
    validate_query_entries_limit, validate_query_range_limit, validate_query_string_bytes_limit,
};

mod apply_nested_vector_aggregation;
mod execute_federated_metric_query;
mod execute_http_logql_expr;
mod execute_http_query_for_tenant;
mod execute_http_variants;
mod http_query_scope;

pub(crate) use apply_nested_vector_aggregation::{
    apply_nested_vector_aggregation, format_float_sample,
};
use execute_federated_metric_query::execute_federated_metric_query;
pub(crate) use execute_http_logql_expr::{LogqlExprScope, execute_http_logql_expr};
pub(crate) use execute_http_query_for_tenant::{
    execute_http_query_for_tenant, execute_http_query_for_tenant_inner,
};
use execute_http_variants::{VariantsDefinition, execute_http_variants};
pub(crate) use http_query_scope::HttpQueryScope;

// A selection keeps full sample labels, while its grouping chooses independent
// ranking buckets. The existing selector also ranks matrix points per timestamp.
struct MetricSelection {
    limit: u64,
    order: SampleOrder,
}

pub(crate) fn apply_grouped_metric_selection(
    value: &mut Value,
    aggregation: &VectorAggregation,
) -> Result<(), HttpQueryError> {
    let selection = match aggregation.op {
        VectorAggregationOp::TopK(limit) => MetricSelection {
            limit,
            order: SampleOrder::Descending,
        },
        VectorAggregationOp::BottomK(limit) => MetricSelection {
            limit,
            order: SampleOrder::Ascending,
        },
        _ => {
            return Err(HttpQueryError::VariantUnsupported(
                "expected topk or bottomk".into(),
            ));
        }
    };
    let mut groups = BTreeMap::<Labels, Vec<Value>>::new();
    let rows = value["data"]["result"]
        .as_array_mut()
        .expect("metric result is an array");
    for row in std::mem::take(rows) {
        let labels = metric_series_labels(&row)
            .ok_or_else(|| HttpQueryError::VariantUnsupported("invalid metric labels".into()))?;
        groups
            .entry(vector_group_labels(&labels, aggregation.grouping.as_ref()))
            .or_default()
            .push(row);
    }
    let mut selected = Vec::new();
    for rows in groups.into_values() {
        let mut group = json!({"data":{"resultType":value["data"]["resultType"],"result":rows}});
        apply_metric_selection(
            &mut group,
            usize::try_from(selection.limit).unwrap_or(usize::MAX),
            selection.order,
        );
        selected.extend(
            group["data"]["result"]
                .as_array_mut()
                .map_or_else(Default::default, std::mem::take),
        );
    }
    value["data"]["result"] = Value::Array(selected);
    Ok(())
}
