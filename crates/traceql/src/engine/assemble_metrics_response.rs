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

    let bucket_count = if metric.instant {
        1
    } else {
        usize::try_from((end_ns.0 - start_ns.0) / step_ns.0 + 1)
            .map_err(|e| TraceqlError::Plan(e.to_string()))?
    };
    let exemplar_rows = sample_exemplar_rows(batches, start_ns, end_ns, metric_policy.0)?;
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
            let (mut labels, mut label_types) = metric_labels(batch, row, &metric.by)?;
            if metric.frontend_labels {
                // A single unsharded scan pools observations under the decoded
                // frontend key before reduction. This retains the sum/count
                // weights for averages and the histogram population for
                // quantiles; merging already reduced values would lose them.
                super::metric_labels::decode_frontend_metric_labels(&mut labels, &mut label_types);
            }
            // Tempo drops partial missing groups but preserves the first
            // "nil" label when all grouping attributes are absent.
            if labels.is_empty()
                && let Some(field) = metric.by.first()
            {
                labels.push((metric_label_key(field), "nil".into()));
            }
            let labels = (labels, label_types);
            let exemplar = if exemplar_rows.contains(&(batch_index, row)) {
                let mut exemplar =
                    metric_exemplar(batch, row, ts.0, exemplar_value, &metric.exemplar_fields)?;
                if metric.frontend_labels {
                    super::metric_labels::decode_frontend_metric_labels(
                        &mut exemplar.labels,
                        &mut exemplar.label_types,
                    );
                }
                Some(exemplar)
            } else {
                None
            };
            let series_buckets = buckets
                .entry(labels)
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
            (Vec::new(), BTreeMap::new()),
            vec![MetricBucket::default(); bucket_count],
        );
    }

    let step = Time::from_nanos(step_ns.0);
    let mut series: Vec<TraceMetricSeries> = buckets
        .into_iter()
        .map(|(labels, buckets)| {
            metric_series_for_group(
                labels,
                buckets,
                metric,
                (output_start_ns.0, step_ns.0),
                step,
                (start_ns.0, end_ns.0),
                metric_policy,
            )
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    if !matches!(
        metric.function,
        MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime
    ) {
        let mut admission =
            super::metric_exemplars::ExemplarBuckets::new(metric_policy.0, start_ns.0, end_ns.0);
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
            for (_, value) in &mut series.points {
                *value *= metric.sampling_factor;
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
