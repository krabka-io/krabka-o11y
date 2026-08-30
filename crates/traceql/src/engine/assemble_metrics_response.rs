use super::*;

pub(crate) fn assemble_metrics_response(
    batches: &[RecordBatch],
    start_ns: UnixNano,
    end_ns: UnixNano,
    step_ns: DurationNanos,
    metric: &MetricPlan,
    metric_policy: (usize, &[Time]),
    output_start_ns: UnixNano,
) -> Result<TraceMetricsResponse> {
    if step_ns.0 <= 0 {
        return Err(TraceqlError::Plan("metrics step must be positive".into()));
    }
    if end_ns < start_ns {
        return Err(TraceqlError::Plan("metrics end must be >= start".into()));
    }

    let bucket_count = usize::try_from((end_ns.0 - start_ns.0) / step_ns.0 + 1)
        .map_err(|e| TraceqlError::Plan(e.to_string()))?;
    let mut buckets: BTreeMap<Vec<(String, String)>, Vec<MetricBucket>> = BTreeMap::new();
    for batch in batches {
        let starts = batch
            .column_by_name(COL_START)
            .ok_or_else(|| TraceqlError::Exec(format!("missing column {COL_START}")))?
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            let ts = UnixNano(starts.value(row));
            if ts < start_ns || ts > end_ns {
                continue;
            }
            let idx = usize::try_from((ts.0 - start_ns.0) / step_ns.0)
                .map_err(|e| TraceqlError::Exec(e.to_string()))?;
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
            let labels = metric_labels(batch, row, &metric.by)?;
            let exemplar = metric_exemplar(batch, row, ts.0, value.unwrap_or(1.0))?;
            let series_buckets = buckets
                .entry(labels)
                .or_insert_with(|| vec![MetricBucket::default(); bucket_count]);
            if let Some(bucket) = series_buckets.get_mut(idx) {
                bucket.record(value, Some(exemplar));
            }
        }
    }
    if buckets.is_empty() {
        buckets.insert(Vec::new(), vec![MetricBucket::default(); bucket_count]);
    }

    let step = Time::from_nanos(step_ns.0);
    let series = buckets
        .into_iter()
        .map(|(labels, buckets)| {
            metric_series_for_group(
                labels,
                buckets,
                metric,
                output_start_ns.0,
                step_ns.0,
                step,
                metric_policy,
            )
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(TraceMetricsResponse {
        series: apply_rank(apply_metric_filter(series, metric.filter), metric.rank),
    })
}
