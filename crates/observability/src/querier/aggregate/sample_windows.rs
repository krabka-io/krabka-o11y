type MetricSamples = BTreeMap<Labels, BTreeMap<i64, MetricSampleState>>;
type FormattedMetricSeries = Vec<(Labels, Vec<[String; 2]>)>;

#[derive(Clone, Copy)]
struct MetricWindow<'a> {
    query: &'a MetricQuery,
    eval_times: &'a [i64],
    range_ns: i64,
    delete_filters: &'a [ActiveLogDeleteFilter],
}

fn merge_metric_samples(samples: &mut MetricSamples, block_samples: MetricSamples) {
    for (labels, values) in block_samples {
        let target = samples.entry(labels).or_default();
        for (timestamp_ns, value) in values {
            let sample = target.entry(timestamp_ns).or_default();
            sample.merge(value);
        }
    }
}

fn apply_absent_over_time(samples: &mut MetricSamples, query: &MetricQuery, eval_times: &[i64]) {
    if !matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return;
    }

    let mut absent_values = BTreeMap::new();
    for eval_time_ns in eval_times {
        let has_sample = samples.values().any(|values| {
            values
                .get(eval_time_ns)
                .is_some_and(MetricSampleState::has_samples)
        });
        if !has_sample {
            let mut sample = MetricSampleState::default();
            sample.record(*eval_time_ns, MetricValue::integer(1));
            absent_values.insert(*eval_time_ns, sample);
        }
    }

    samples.clear();
    if !absent_values.is_empty() {
        samples.insert(absent_metric_labels(query), absent_values);
    }
}

fn absent_metric_labels(query: &MetricQuery) -> Labels {
    query
        .stream
        .matchers
        .iter()
        .filter(|matcher| matcher.op == MatchOp::Equal)
        .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
        .collect::<Labels>()
}

fn metric_samples_from_batches(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_times: &[i64],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<MetricSamples, QueryError> {
    let mut samples: MetricSamples = BTreeMap::new();

    for batch in batches {
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "series_fingerprint",
                expected: "UInt64",
            })?;
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "timestamp_ns",
                expected: "Int64",
            })?;
        let lines = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(QueryError::InvalidColumn {
                column: "line",
                expected: "Utf8",
            })?;
        let metadata = batch.column(3).as_any().downcast_ref::<MapArray>().ok_or(
            QueryError::InvalidColumn {
                column: "structured_metadata",
                expected: "Map<Utf8, Utf8>",
            },
        )?;

        for row in 0..batch.num_rows() {
            let structured_metadata = structured_metadata_value(metadata, row)?;
            append_matching_metric_row(
                &mut samples,
                plan,
                label_index,
                QueryRow {
                    fingerprint: fingerprints.value(row),
                    timestamp_ns: timestamps.value(row),
                    line: lines.value(row),
                    structured_metadata: &structured_metadata,
                },
                MetricWindow {
                    query,
                    eval_times,
                    range_ns: query.range_ns.0,
                    delete_filters,
                },
            )?;
        }
    }

    Ok(samples)
}

fn format_metric_samples(samples: MetricSamples, query: &MetricQuery) -> FormattedMetricSeries {
    let samples = if let Some(grouping) = &query.range_grouping {
        group_range_samples(samples, grouping)
    } else {
        samples
    };

    if let Some(vector_aggregation) = &query.vector_aggregation {
        let mut series = aggregate_vector_samples(samples, query, vector_aggregation)
            .into_iter()
            .map(|(labels, values)| {
                (
                    labels,
                    values
                        .into_iter()
                        .map(|(time, value)| [time.to_string(), format_metric_value(value)])
                        .collect(),
                )
            })
            .collect::<FormattedMetricSeries>();
        sort_formatted_vector_samples(&mut series, &vector_aggregation.op);
        return series;
    }

    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| {
                        [
                            time.to_string(),
                            format_metric_value(range_sample_value(value, query)),
                        ]
                    })
                    .collect(),
            )
        })
        .collect()
}

fn sort_formatted_vector_samples(series: &mut FormattedMetricSeries, op: &VectorAggregationOp) {
    match op {
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            series.sort_by(|left, right| {
                let left_value = left
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let right_value = right
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let value_order = match op {
                    VectorAggregationOp::Sort => left_value.cmp_value(right_value),
                    VectorAggregationOp::SortDesc => right_value.cmp_value(left_value),
                    _ => Ordering::Equal,
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
        }
        _ => {}
    }
}

fn group_range_samples(samples: MetricSamples, grouping: &VectorGrouping) -> MetricSamples {
    let mut grouped: MetricSamples = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, Some(grouping));
        let grouped_values = grouped.entry(grouped_labels).or_default();
        for (time, value) in values {
            grouped_values.entry(time).or_default().merge(value);
        }
    }

    grouped
}

fn aggregate_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    vector_aggregation: &VectorAggregation,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    match &vector_aggregation.op {
        VectorAggregationOp::TopK(limit) | VectorAggregationOp::ApproxTopK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Largest,
            );
        }
        VectorAggregationOp::BottomK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Smallest,
            );
        }
        VectorAggregationOp::CountValues(label) => {
            return count_values_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                label,
            );
        }
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            return select_all_vector_samples(samples, query);
        }
        _ => {}
    }

    let mut states: BTreeMap<Labels, BTreeMap<i64, VectorAggregationState>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, vector_aggregation.grouping.as_ref());
        for (time, value) in values {
            states
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .record(range_sample_value(value, query));
        }
    }

    states
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, state)| (time, state.finish(&vector_aggregation.op)))
                    .collect(),
            )
        })
        .collect()
}

fn count_values_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    value_label: &str,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut counted: BTreeMap<Labels, BTreeMap<i64, u64>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            let value = range_sample_value(value, query);
            let mut output_labels = grouped_labels.clone();
            output_labels.insert(value_label.to_string(), format_metric_value(value));
            *counted
                .entry(output_labels)
                .or_default()
                .entry(time)
                .or_default() += 1;
        }
    }

    counted
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, count)| (time, MetricValue::integer(count)))
                    .collect(),
            )
        })
        .collect()
}

fn select_all_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| (time, range_sample_value(value, query)))
                    .collect(),
            )
        })
        .collect()
}

#[derive(Clone, Copy)]
enum VectorSelection {
    Largest,
    Smallest,
}

fn select_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    limit: u64,
    selection: VectorSelection,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut groups: BTreeMap<Labels, BTreeMap<i64, Vec<(Labels, MetricValue)>>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            groups
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .push((labels.clone(), range_sample_value(value, query)));
        }
    }

    let mut selected = BTreeMap::new();
    for (_grouped_labels, values) in groups {
        for (time, mut candidates) in values {
            candidates.sort_by(|left, right| {
                let value_order = match selection {
                    VectorSelection::Largest => right.1.cmp_value(left.1),
                    VectorSelection::Smallest => left.1.cmp_value(right.1),
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            for (labels, value) in candidates.into_iter().take(limit) {
                selected
                    .entry(labels)
                    .or_insert_with(BTreeMap::new)
                    .insert(time, value);
            }
        }
    }

    selected
}

fn vector_group_labels(labels: &Labels, grouping: Option<&VectorGrouping>) -> Labels {
    match grouping {
        Some(VectorGrouping::By(names)) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(VectorGrouping::Without(names)) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        None => Labels::new(),
    }
}

fn range_sample_value(value: MetricSampleState, query: &MetricQuery) -> MetricValue {
    match query.aggregation {
        RangeAggregation::CountOverTime
        | RangeAggregation::BytesOverTime
        | RangeAggregation::AbsentOverTime
        | RangeAggregation::SumOverTime => value.sum,
        RangeAggregation::PresentOverTime => MetricValue::integer(1),
        RangeAggregation::Rate | RangeAggregation::BytesRate => {
            rate_metric_value(value.sum, query.range_ns.0)
        }
        RangeAggregation::RateCounter => {
            rate_metric_value(value.counter_increase(), query.range_ns.0)
        }
        RangeAggregation::AvgOverTime => value.average(),
        RangeAggregation::StdvarOverTime => value.stdvar(),
        RangeAggregation::StddevOverTime => value.stddev(),
        RangeAggregation::QuantileOverTime(quantile) => value.quantile(quantile),
        RangeAggregation::MinOverTime => value.min.unwrap_or_else(MetricValue::zero),
        RangeAggregation::MaxOverTime => value.max.unwrap_or_else(MetricValue::zero),
        RangeAggregation::FirstOverTime => value
            .first
            .map_or_else(MetricValue::zero, |(_, value)| value),
        RangeAggregation::LastOverTime => value
            .last
            .map_or_else(MetricValue::zero, |(_, value)| value),
    }
}

fn is_unwrapped_metric_query(query: &MetricQuery) -> bool {
    query
        .stream
        .pipeline
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Unwrap(_)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetricValue {
    numerator: i128,
    denominator: u128,
}

const METRIC_DECIMAL_SCALE: u128 = 1_000_000_000;

