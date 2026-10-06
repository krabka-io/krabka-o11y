use num_traits::ToPrimitive as _;

use super::{apply_grouped_metric_selection, apply_nested_vector_aggregation};
use crate::{
    HttpQueryError, Labels, MetricQuery, PipelineStage, QuerierState, QueryKind, RangeAggregation,
    TimeRange, Value, VectorAggregationOp, add_loki_query_stats, default_metric_range_step,
    eval_times,
    http::params_format::aggregation_formatting::apply_approx_metric_selection,
    json, loki_matrix_response, loki_vector_response_from_matrix, merge_loki_query_response,
    metric_scan_range,
    querier::{
        aggregate::sample_windows::absent_metric_labels,
        metric_eval::scalar_samples::execute_http_metric_query_with_scan_range,
    },
};

// Loki's MultiTenantQuerier adds tenant labels to extracted samples before
// the one query-wide vector evaluator. Local top-k or sum cannot precede this
// merge: it would lose contributions from another tenant or shard.
pub(crate) async fn execute_federated_metric_query(
    state: &QuerierState,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    mut query: MetricQuery,
    scan_range: Option<TimeRange>,
) -> Result<Value, HttpQueryError> {
    let aggregation = query.vector_aggregation.take();
    let absent_labels = matches!(query.aggregation, RangeAggregation::AbsentOverTime)
        .then(|| absent_metric_labels(&query));
    if absent_labels.is_some() {
        query.aggregation = RangeAggregation::CountOverTime;
    }
    validate_approximate_aggregation(state, kind, aggregation.as_ref())?;
    let tenants = state
        .federated_metric_tenants
        .as_ref()
        .expect("federated request has authorized tenants");
    let scan_range = scan_range.map_or_else(|| metric_scan_range(&query, time_range), Ok)?;
    let mut response = add_loki_query_stats(json!({"status":"success","data":{
        "resultType":if matches!(kind, QueryKind::Instant) { "vector" } else { "matrix" },"result":[]
    }}));
    for tenant in tenants.iter() {
        let tenant_labels = Labels::from([("__tenant_id__".into(), tenant.as_str().into())]);
        if !query
            .stream
            .matchers
            .iter()
            .filter(|matcher| matcher.name == "__tenant_id__")
            .all(|matcher| matcher.matches(&tenant_labels))
        {
            continue;
        }
        let mut query = query.clone();
        // Loki's variants retain the original Plan AST when the federation
        // wrapper rewrites Selector. Its physical matcher therefore remains,
        // unlike an ordinary metric query's rewritten downstream selector.
        if !query
            .stream
            .pipeline
            .contains(&PipelineStage::VariantBoundary)
        {
            query
                .stream
                .matchers
                .retain(|matcher| matcher.name != "__tenant_id__");
            for matcher in &mut query.stream.matchers {
                if matcher.name == "original___tenant_id__" {
                    matcher.name = "__tenant_id__".into();
                }
            }
        }
        let mut tenant_state = state.clone();
        tenant_state.federated_metric_tenants = None;
        tenant_state = tenant_state.with_tenant_limits(tenant);
        let mut value = execute_http_metric_query_with_scan_range(
            &tenant_state,
            tenant.as_str(),
            time_range,
            step,
            kind,
            query,
            Some(scan_range),
        )
        .await?;
        add_federated_tenant_labels(&mut value, tenant.as_str());
        merge_loki_query_response(&mut response, &value);
    }
    if let Some(labels) = absent_labels {
        apply_federated_absence(&mut response, labels, time_range, step, kind);
    }
    if let Some(aggregation) = aggregation {
        match aggregation.op {
            VectorAggregationOp::ApproxTopK(limit) => apply_approx_metric_selection(
                &mut response,
                usize::try_from(limit).unwrap_or(usize::MAX),
                state.max_count_min_sketch_heap_size,
            ),
            VectorAggregationOp::TopK(_) | VectorAggregationOp::BottomK(_) => {
                apply_grouped_metric_selection(&mut response, &aggregation)?;
            }
            _ => apply_nested_vector_aggregation(&mut response, &aggregation)?,
        }
    }
    Ok(response)
}

fn validate_approximate_aggregation(
    state: &QuerierState,
    kind: QueryKind,
    aggregation: Option<&crate::VectorAggregation>,
) -> Result<(), HttpQueryError> {
    if aggregation
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::ApproxTopK(_)))
    {
        if !state
            .limits
            .shard_aggregations
            .iter()
            .any(|name| name == "approx_topk")
        {
            return Err(HttpQueryError::ApproxTopKDisabled);
        }
        if matches!(kind, QueryKind::Range) {
            return Err(HttpQueryError::ApproxTopKRangeQuery);
        }
    }
    Ok(())
}

fn add_federated_tenant_labels(value: &mut Value, tenant: &str) {
    for row in value["data"]["result"]
        .as_array_mut()
        .expect("metric result is an array")
    {
        let labels = row["metric"].as_object_mut().expect("sample has labels");
        if let Some(original) = labels.remove("__tenant_id__") {
            labels.insert("original___tenant_id__".into(), original);
        }
        labels.insert("__tenant_id__".into(), Value::String(tenant.into()));
    }
}

// Absence is computed after merging real samples from every selected tenant.
// Synthesizing once per tenant would report an absent series even when another
// tenant supplied matching data in that same evaluation window.
fn apply_federated_absence(
    response: &mut Value,
    labels: Labels,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
) {
    let mut present = std::collections::BTreeSet::new();
    for row in response["data"]["result"]
        .as_array()
        .expect("metric result is an array")
    {
        let points = row["values"]
            .as_array()
            .map_or_else(|| std::slice::from_ref(&row["value"]), Vec::as_slice);
        for point in points {
            if let Some(time) = point[0].as_f64() {
                present.insert(time.to_bits());
            }
        }
    }
    let evaluation = if matches!(kind, QueryKind::Instant) {
        TimeRange::new(time_range.end_ns, time_range.end_ns).expect("instant range")
    } else {
        time_range
    };
    let step_ns = if matches!(kind, QueryKind::Instant) {
        1
    } else {
        step.unwrap_or_else(|| default_metric_range_step(time_range))
    };
    let points = eval_times(evaluation, step_ns)
        .into_iter()
        .filter(|time| {
            !present
                .contains(&((time.to_f64().expect("i64 converts to finite f64") / 1e9).to_bits()))
        })
        .map(|time| [time.to_string(), "1".to_string()])
        .collect::<Vec<_>>();
    let mut value = loki_matrix_response(if points.is_empty() {
        Vec::new()
    } else {
        vec![(labels, points)]
    });
    if matches!(kind, QueryKind::Instant) {
        value = loki_vector_response_from_matrix(value);
    }
    response["data"]["result"] = value["data"]["result"].take();
}
