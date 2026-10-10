use std::collections::BTreeSet;

use krabka_logql::{
    DurationNanos, OffsetNanos, PipelineStage, StreamQuery, VectorAggregation, VectorAggregationOp,
    VectorGrouping,
};

use super::{
    HttpQueryScope, apply_grouped_metric_selection, apply_nested_vector_aggregation,
    execute_federated_metric_query,
};
use crate::{
    HttpQueryError, LogqlExpr, QuerierState, QueryKind, Value, add_loki_query_stats,
    http::params_format::aggregation_formatting::apply_approx_metric_selection,
    json, merge_loki_query_stats, metric_scan_range,
    querier::{
        aggregate::sample_windows::absent_metric_labels,
        metric_eval::scalar_samples::{HttpMetricQuery, execute_http_metric_query},
    },
};

/// The parts of one `variants(...) of (...)` expression.
#[derive(Clone, Copy)]
pub(crate) struct VariantsDefinition<'a> {
    pub(crate) variants: &'a [LogqlExpr],
    pub(crate) stream: &'a StreamQuery,
    pub(crate) range_ns: DurationNanos,
    pub(crate) offset_ns: OffsetNanos,
}

// Boundaries and extractor composition follow Loki 3.7.7's
// DefaultEvaluator.NewVariantsStepEvaluator and MultiVariantExpr.extractor.
pub(crate) async fn execute_http_variants(
    scope: HttpQueryScope<'_>,
    definition: VariantsDefinition<'_>,
) -> Result<Value, HttpQueryError> {
    let HttpQueryScope {
        state,
        tenant,
        time_range,
        step,
        kind,
    } = scope;
    let VariantsDefinition {
        variants,
        stream,
        range_ns,
        offset_ns,
    } = definition;
    if !state.limits.enable_multi_variant_queries {
        return Err(HttpQueryError::VariantsDisabled);
    }
    let mut response = add_loki_query_stats(json!({"status":"success","data":{
        "resultType": if matches!(kind, QueryKind::Instant) { "vector" } else { "matrix" },
        "result":[]
    }}));
    let mut results = Vec::new();
    let mut warnings = Vec::new();
    for (index, expression) in variants.iter().enumerate() {
        let (mut query, mut aggregation) = variant_metric(expression)?;
        let absent_labels = matches!(query.aggregation, crate::RangeAggregation::AbsentOverTime)
            .then(|| absent_metric_labels(&query));
        let mut pipeline = stream.pipeline.clone();
        pipeline.push(PipelineStage::VariantBoundary);
        if aggregation.is_some() {
            pipeline.extend(query.stream.pipeline);
        } else if let Some(at) = query
            .stream
            .pipeline
            .iter()
            .position(|stage| matches!(stage, PipelineStage::Unwrap(_)))
        {
            pipeline.extend(query.stream.pipeline.into_iter().skip(at));
        }
        query.stream = StreamQuery {
            matchers: stream.matchers.clone(),
            pipeline,
        };
        let mut common_query = query.clone();
        common_query.range_ns = range_ns;
        common_query.offset_ns = offset_ns;
        let scan_range = metric_scan_range(&common_query, time_range)?;
        let metric_query = HttpMetricQuery {
            time_range,
            step,
            kind,
            query,
            common_scan_range: Some(scan_range),
        };
        let mut value = if state.federated_metric_tenants.is_some() {
            execute_federated_metric_query(state, metric_query).await?
        } else {
            execute_http_metric_query(state, tenant, metric_query).await?
        };
        let variant = index.to_string();
        if let Some(rows) = value["data"]["result"].as_array_mut() {
            for row in rows {
                if let Some(labels) = &absent_labels {
                    // The range evaluator synthesizes absence from its own
                    // selector. No extracted sample exists to carry __variant__.
                    row["metric"] = json!(labels);
                } else {
                    row["metric"]["__variant__"] = Value::String(variant.clone());
                }
            }
        }
        if let Some(aggregation) = aggregation.as_mut() {
            apply_variant_aggregation(state, kind, &mut value, aggregation)?;
        }
        merge_loki_query_stats(&mut response["data"]["stats"], &value["data"]["stats"]);
        let rows = value["data"]["result"]
            .as_array_mut()
            .expect("metric result is an array");
        // The instant branch returns the original vector; the range branch
        // materializes only series carrying the variant label (engine.go:609).
        if matches!(kind, QueryKind::Range) {
            rows.retain(|row| row["metric"]["__variant__"].is_string());
        }
        if variant_series_limit_exceeded(
            rows,
            usize::try_from(state.limits.max_query_series).unwrap_or(usize::MAX),
        ) {
            warnings.push(Value::String(format!(
                "maximum of series ({}) reached for variant ({variant})",
                state.limits.max_query_series
            )));
        } else {
            results.append(rows);
        }
    }
    results.sort_by_cached_key(|row| row["metric"].to_string());
    response["data"]["result"] = Value::Array(results);
    if !warnings.is_empty() {
        response["warnings"] = Value::Array(warnings);
    }
    Ok(response)
}

fn apply_variant_aggregation(
    state: &QuerierState,
    kind: QueryKind,
    value: &mut Value,
    aggregation: &mut VectorAggregation,
) -> Result<(), HttpQueryError> {
    match &mut aggregation.grouping {
        Some(VectorGrouping::By(labels) | VectorGrouping::Without(labels)) => {
            labels.push("__variant__".to_string());
        }
        None => aggregation.grouping = Some(VectorGrouping::By(vec!["__variant__".to_string()])),
    }
    match &aggregation.op {
        VectorAggregationOp::ApproxTopK(limit) => {
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
            apply_approx_metric_selection(
                value,
                usize::try_from(*limit).unwrap_or(usize::MAX),
                state.max_count_min_sketch_heap_size,
            );
        }
        VectorAggregationOp::TopK(_) | VectorAggregationOp::BottomK(_) => {
            apply_grouped_metric_selection(value, aggregation)?;
        }
        _ => apply_nested_vector_aggregation(value, aggregation)?,
    }
    Ok(())
}

fn variant_metric(
    expression: &LogqlExpr,
) -> Result<(krabka_logql::MetricQuery, Option<VectorAggregation>), HttpQueryError> {
    match expression {
        LogqlExpr::Metric { query, .. } => {
            let mut query = query.clone();
            let aggregation = query.vector_aggregation.take();
            Ok((query, aggregation))
        }
        LogqlExpr::Aggregation {
            expr, aggregation, ..
        } => {
            if let LogqlExpr::Metric { query, .. } = expr.as_ref()
                && query.vector_aggregation.is_none()
            {
                return Ok((query.clone(), Some(aggregation.clone())));
            }
            Err(HttpQueryError::VariantUnsupported(
                "expected range aggregation expression".to_string(),
            ))
        }
        _ => Err(HttpQueryError::VariantUnsupported(
            "unsupported variant evaluator".to_string(),
        )),
    }
}

// Loki checks the size BEFORE each sample, including repeat samples of an
// existing series. Reaching the ceiling and visiting a later time therefore
// removes that entire variant, rather than returning its earlier points.
fn variant_series_limit_exceeded(rows: &[Value], maximum: usize) -> bool {
    let mut samples = Vec::new();
    for row in rows {
        if !row["metric"]["__variant__"].is_string() {
            continue;
        }
        let identity = row["metric"].to_string();
        if let Some(points) = row["values"].as_array() {
            for point in points {
                samples.push((point[0].as_f64().unwrap_or_default(), identity.clone()));
            }
        } else if let Some(point) = row["value"].as_array() {
            samples.push((point[0].as_f64().unwrap_or_default(), identity));
        }
    }
    samples.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let mut seen = BTreeSet::new();
    for (_, identity) in samples {
        if seen.len() >= maximum {
            return true;
        }
        seen.insert(identity);
    }
    false
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::variant_series_limit_exceeded;
    use crate::json;

    #[test]
    fn variant_ceiling_checks_repeated_points_and_zero_without_cross_variant_budget() {
        let two = vec![
            json!({"metric":{"app":"a","__variant__":"0"},"value":[1,"3"]}),
            json!({"metric":{"app":"b","__variant__":"0"},"value":[1,"4"]}),
        ];
        assert!(!variant_series_limit_exceeded(&two, 2));
        assert!(variant_series_limit_exceeded(&two, 1));
        assert!(variant_series_limit_exceeded(&two, 0));
        assert!(!variant_series_limit_exceeded(&[], 0));
        let repeated =
            vec![json!({"metric":{"app":"a","__variant__":"0"},"values":[[1,"3"],[2,"4"]]})];
        assert!(variant_series_limit_exceeded(&repeated, 1));
        assert!(!variant_series_limit_exceeded(&repeated, 2));
    }
}
