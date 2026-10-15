use krabka_units::convert::TimeExt as _;

use super::{
    BTreeMap, CompareSpec, HashSet, MetricsRange, RecordBatch, Result, SpanIdentity, Time,
    TraceMetricsResponse, TraceqlError, accumulate_compare_counts, apply_metric_filter, apply_rank,
    build_compare_series, metric_exemplar, metric_filter, metric_label_key, metric_labels,
    metric_log2_bucket_value, metric_numeric_value, metric_series_for_group,
    metrics::{
        ExemplarBuckets, ExemplarRange, MetricBucket, MetricExemplarPolicy, MetricFunction,
        MetricGrid, MetricLabels, MetricPlan,
    },
    rank_limit, sample_exemplar_rows,
    sample_exemplar_rows::ExemplarRow,
    span_start_column,
};
use crate::{
    ast::Pipeline,
    ids::{DurationNanos, UnixNano},
    result::{MetricLabel, TraceMetricSeries},
};

// Assemble Compare Response
pub(crate) fn assemble_compare_response(
    batches: &[RecordBatch],
    compare: &CompareSpec,
    range: MetricsRange,
    max_values_per_attr: usize,
    selected_spans: Option<&HashSet<SpanIdentity>>,
) -> Result<TraceMetricsResponse> {
    if range.step.0 <= 0 {
        return Err(TraceqlError::Plan("metrics step must be positive".into()));
    }
    if range.scan_end < range.scan_start {
        return Err(TraceqlError::Plan("metrics end must be >= start".into()));
    }
    let bucket_count = if range.instant {
        1
    } else {
        usize::try_from((range.scan_end.0 - range.scan_start.0) / range.step.0 + 1)
            .map_err(|e| TraceqlError::Plan(e.to_string()))?
    };

    let accumulation = accumulate_compare_counts(
        batches,
        compare,
        range,
        bucket_count,
        max_values_per_attr,
        selected_spans,
    )?;
    let series = build_compare_series(
        accumulation.counts,
        &accumulation.totals,
        compare.top_n,
        range,
        bucket_count,
    );
    Ok(TraceMetricsResponse { series })
}

// Assemble Metrics Response
pub(crate) fn assemble_metrics_response(
    batches: &[RecordBatch],
    start_ns: UnixNano,
    end_ns: UnixNano,
    step_ns: DurationNanos,
    metric: &MetricPlan,
    metric_policy: MetricExemplarPolicy<'_>,
    output_start_ns: UnixNano,
) -> Result<TraceMetricsResponse> {
    if step_ns.0 <= 0 {
        return Err(TraceqlError::Plan("metrics step must be positive".into()));
    }
    if end_ns < start_ns {
        return Err(TraceqlError::Plan("metrics end must be >= start".into()));
    }

    let bucket_count = if metric.instant {
        1
    } else {
        usize::try_from((end_ns.0 - start_ns.0) / step_ns.0 + 1)
            .map_err(|e| TraceqlError::Plan(e.to_string()))?
    };
    let exemplar_rows = sample_exemplar_rows(
        batches,
        start_ns,
        end_ns,
        metric_policy.max_exemplars_per_series,
    )?;
    let mut buckets: BTreeMap<MetricLabels, Vec<MetricBucket>> = BTreeMap::new();
    for (batch_index, batch) in batches.iter().enumerate() {
        let starts = span_start_column(batch)?;
        for row in 0..batch.num_rows() {
            let ts = UnixNano(starts.value(row));
            if ts < start_ns || ts > end_ns {
                continue;
            }
            let idx = if metric.instant {
                0
            } else {
                usize::try_from((ts.0 - start_ns.0) / step_ns.0)
                    .map_err(|e| TraceqlError::Exec(e.to_string()))?
            };
            let value = match metric.value.as_ref() {
                // A metric with a value field (avg/min/max/sum/histogram/...)
                // only observes spans where that attribute is present. A row
                // whose value field is NULL means the attribute is absent, so
                // the span is skipped entirely rather than folded as 0 — it
                // must not drag min toward 0, bias avg, or add a 0 observation
                // to a histogram bucket.
                Some(field) => match metric_numeric_value(batch, row, field)? {
                    Some(value) => Some(value),
                    None => continue,
                },
                // Value-less metrics (count_over_time / rate) observe every
                // matching span regardless of any value field.
                None => None,
            };
            let exemplar_value = if matches!(metric.function, MetricFunction::HistogramOverTime) {
                f64::NAN
            } else {
                value.unwrap_or(f64::NAN)
            };
            let value = if matches!(
                metric.function,
                MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime
            ) {
                match metric_log2_bucket_value(
                    batch,
                    row,
                    metric
                        .value
                        .as_ref()
                        .expect("histograms have a value field"),
                )? {
                    Some(value) => Some(value),
                    None => continue,
                }
            } else {
                value
            };
            let mut metric_labels = metric_labels(batch, row, &metric.by)?;
            if metric.frontend_labels {
                // A single unsharded scan pools observations under the decoded
                // frontend key before reduction. This retains the sum/count
                // weights for averages and the histogram population for
                // quantiles; merging already reduced values would lose them.
                super::metrics::decode_frontend_metric_labels(
                    &mut metric_labels.labels,
                    &mut metric_labels.types,
                );
            }
            // Tempo drops partial missing groups but preserves the first
            // "nil" label when all grouping attributes are absent.
            if metric_labels.labels.is_empty()
                && let Some(field) = metric.by.first()
            {
                metric_labels
                    .labels
                    .push(MetricLabel::new(metric_label_key(field), "nil"));
            }
            let exemplar = if exemplar_rows.contains(&ExemplarRow {
                batch_index,
                row_index: row,
            }) {
                let mut exemplar =
                    metric_exemplar(batch, row, ts.0, exemplar_value, &metric.exemplar_fields)?;
                if metric.frontend_labels {
                    super::metrics::decode_frontend_metric_labels(
                        &mut exemplar.labels,
                        &mut exemplar.label_types,
                    );
                }
                Some(exemplar)
            } else {
                None
            };
            let series_buckets = buckets
                .entry(metric_labels)
                .or_insert_with(|| vec![MetricBucket::default(); bucket_count]);
            if let Some(bucket) = series_buckets.get_mut(idx) {
                bucket.record(value, exemplar);
            }
        }
    }

    // Tempo eagerly initializes ungrouped instant count/rate aggregators,
    // including windows with no observations. Range queries retain zeroes
    // only when a consumed spanset was rejected by a scalar pipeline.
    if buckets.is_empty()
        && (metric.instant || metric.spanset_pipeline_had_input)
        && metric.by.is_empty()
        && matches!(
            metric.function,
            MetricFunction::CountOverTime | MetricFunction::Rate
        )
    {
        buckets.insert(
            MetricLabels {
                labels: Vec::new(),
                types: BTreeMap::new(),
            },
            vec![MetricBucket::default(); bucket_count],
        );
    }

    let step = Time::from_nanos(step_ns.0);
    let mut grouped_series = Vec::new();
    let labels = buckets.keys().cloned().collect::<Vec<_>>();
    for labels in labels {
        let group_buckets = buckets
            .remove(&labels)
            .expect("a collected metric label key exists");
        grouped_series.push(metric_series_for_group(
            labels,
            &group_buckets,
            metric,
            MetricGrid {
                start_ns: output_start_ns.0,
                step_ns: step_ns.0,
            },
            step,
            ExemplarRange {
                start_ns: start_ns.0,
                end_ns: end_ns.0,
            },
            metric_policy,
        )?);
    }
    let mut series: Vec<TraceMetricSeries> = grouped_series.into_iter().flatten().collect();
    if !matches!(
        metric.function,
        MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime
    ) {
        let mut admission =
            ExemplarBuckets::new(metric_policy.max_exemplars_per_series, start_ns.0, end_ns.0);
        for series in &mut series {
            series
                .exemplars
                .retain(|exemplar| admission.admit(exemplar.timestamp_ns));
        }
    }
    if metric.sampling_factor > 1.0
        && !matches!(
            metric.function,
            MetricFunction::AvgOverTime
                | MetricFunction::MinOverTime
                | MetricFunction::MaxOverTime
                | MetricFunction::QuantileOverTime
        )
    {
        for series in &mut series {
            for point in &mut series.points {
                point.value *= metric.sampling_factor;
            }
        }
    }
    for stage in &metric.stages {
        series = match stage {
            Pipeline::Filter { op, value } => {
                apply_metric_filter(series, Some(metric_filter(*op, *value)?))
            }
            Pipeline::TopK(_) | Pipeline::BottomK(_) => {
                apply_rank(series, Some(rank_limit(stage)?))
            }
            _ => unreachable!("metric plan validates second stages"),
        };
    }
    Ok(TraceMetricsResponse { series })
}

use super::{
    ByteSize, COL_DURATION, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND,
    COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME,
    COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START, COL_STATUS_CODE, COL_STATUS_MESSAGE,
    COL_TRACE_DURATION, COL_TRACE_ID, COL_TRACE_START, SearchResponse, SpanRef, SpanSet,
    TraceResult, deduplicate_search_spans, fixed_8, fixed_16, i32_value, i64_value,
    optional_fixed_8, row_attrs, string_value, u64_from_i64,
};
pub(crate) fn assemble_search_response(
    batches: &[RecordBatch],
    limit: usize,
    spss: usize,
    most_recent: bool,
    inspected: ByteSize,
) -> Result<SearchResponse> {
    let mut traces: BTreeMap<[u8; 16], TraceAcc> = BTreeMap::new();
    let mut groups: BTreeMap<[u8; 16], BTreeMap<String, SpanSet>> = BTreeMap::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let trace_id = fixed_16(batch, COL_TRACE_ID, row)?;
            let span = SpanRef {
                span_id: fixed_8(batch, COL_SPAN_ID, row)?,
                parent_span_id: optional_fixed_8(batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(batch, COL_NAME, row).unwrap_or_default(),
                kind: i32_value(batch, COL_KIND, row)?,
                nested_set_left: i32_value(batch, COL_NS_LEFT, row)?,
                nested_set_right: i32_value(batch, COL_NS_RIGHT, row)?,
                nested_set_parent: i32_value(batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64_from_i64(i64_value(batch, COL_START, row)?)?,
                duration: Time::from_nanos(i64_value(batch, COL_DURATION, row)?),
                status_code: i32_value(batch, COL_STATUS_CODE, row)?,
                status_message: string_value(batch, COL_STATUS_MESSAGE, row).unwrap_or_default(),
                instrumentation_name: string_value(batch, COL_INSTRUMENTATION_NAME, row)
                    .unwrap_or_default(),
                instrumentation_version: string_value(batch, COL_INSTRUMENTATION_VERSION, row)
                    .unwrap_or_default(),
                resource_attributes: Vec::new(),
                attributes: row_attrs(batch, row)?,
                events: Vec::new(),
                links: Vec::new(),
            };
            if let Some(group_id) = string_value(batch, "__traceql.spanset", row) {
                let group = groups
                    .entry(trace_id)
                    .or_default()
                    .entry(group_id)
                    .or_insert_with(|| SpanSet {
                        spans: Vec::new(),
                        matched: 0,
                        attributes: Vec::new(),
                    });
                if group.spans.is_empty()
                    && let Some(encoded) = string_value(batch, "__traceql.spanset_attributes", row)
                {
                    group.attributes = serde_json::from_str(&encoded)
                        .map_err(|error| TraceqlError::Exec(error.to_string()))?;
                }
                group.spans.push(span.clone());
            }
            traces
                .entry(trace_id)
                .or_insert_with(|| TraceAcc {
                    root_service_name: string_value(batch, COL_ROOT_SERVICE_NAME, row)
                        .unwrap_or_default(),
                    root_trace_name: string_value(batch, COL_ROOT_SPAN_NAME, row)
                        .unwrap_or_default(),
                    start_time_unix_nano: u64_from_i64(
                        i64_value(batch, COL_TRACE_START, row).unwrap_or_default(),
                    )
                    .unwrap_or_default(),
                    duration: Time::from_nanos(
                        i64_value(batch, COL_TRACE_DURATION, row).unwrap_or_default(),
                    ),
                    spans: Vec::new(),
                })
                .spans
                .push(span);
        }
    }

    let mut out: Vec<TraceResult> = traces
        .into_iter()
        .map(|trace_entry| {
            let trace_id = trace_entry.0;
            let mut acc = trace_entry.1;
            deduplicate_search_spans(&mut acc.spans);
            let matched = u32::try_from(acc.spans.len()).unwrap_or(u32::MAX);
            let grouped = groups
                .get(&trace_id)
                .into_iter()
                .flat_map(|groups| groups.values())
                .map(|group| {
                    let mut spans = group.spans.clone();
                    deduplicate_search_spans(&mut spans);
                    let matched = u32::try_from(spans.len()).unwrap_or(u32::MAX);
                    SpanSet {
                        spans: spans.into_iter().take(spss).collect(),
                        matched,
                        attributes: group.attributes.clone(),
                    }
                })
                .collect::<Vec<_>>();
            let span_sets = if grouped.is_empty() {
                vec![SpanSet {
                    spans: acc.spans.into_iter().take(spss).collect(),
                    matched,
                    attributes: Vec::new(),
                }]
            } else {
                grouped
            };
            TraceResult {
                trace_id,
                root_service_name: acc.root_service_name,
                root_trace_name: acc.root_trace_name,
                start_time_unix_nano: acc.start_time_unix_nano,
                duration: acc.duration,
                span_sets,
            }
        })
        .collect();
    let inspected_traces = out.len();
    if most_recent {
        out.sort_by(|a, b| {
            b.start_time_unix_nano
                .cmp(&a.start_time_unix_nano)
                .then_with(|| a.trace_id.cmp(&b.trace_id))
        });
    } else {
        out.sort_by(|left, right| {
            left.start_time_unix_nano
                .cmp(&right.start_time_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
    }
    out.truncate(limit);
    Ok(SearchResponse {
        traces: out,
        inspected_traces,
        inspected,
    })
}

pub(crate) struct TraceAcc {
    pub(crate) root_service_name: String,
    pub(crate) root_trace_name: String,
    pub(crate) start_time_unix_nano: u64,
    pub(crate) duration: Time,
    pub(crate) spans: Vec<SpanRef>,
}
