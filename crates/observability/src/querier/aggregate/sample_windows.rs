use crate::{
    ActiveLogDeleteFilter, BTreeMap, LabelIndex, Labels, MatchOp, MetricQuery, MetricSampleState,
    Ordering, PipelineStage, QueryError, RangeAggregation, StreamPlan, VectorAggregation,
    VectorAggregationOp, VectorAggregationState, VectorGrouping, append_matching_metric_row,
    for_each_query_row, format_metric_value, parse_metric_sample_value, rate_metric_value,
};

pub(crate) const METRIC_DECIMAL_SCALE: u128 = 1_000_000_000;

pub(crate) fn absent_metric_labels(query: &MetricQuery) -> Labels {
    let mut labels = Labels::new();
    for matcher in &query.stream.matchers {
        if matcher.op == MatchOp::Equal {
            labels.insert(matcher.name.clone(), matcher.value.clone());
        }
    }
    labels
}

pub(crate) fn is_unwrapped_metric_query(query: &MetricQuery) -> bool {
    query
        .stream
        .pipeline
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Unwrap(_)))
}

pub(crate) fn merge_metric_samples(samples: &mut MetricSamples, block_samples: MetricSamples) {
    for series in block_samples {
        let target = samples.entry(series.0).or_default();
        for sample_entry in series.1 {
            let sample = target.entry(sample_entry.0).or_default();
            sample.merge(sample_entry.1);
        }
    }
}

pub(crate) fn group_range_samples(
    samples: MetricSamples,
    grouping: &VectorGrouping,
) -> MetricSamples {
    let mut grouped: MetricSamples = BTreeMap::new();

    for series in samples {
        let labels = series.0;
        let values = series.1;
        let grouped_labels = vector_group_labels(&labels, Some(grouping));
        let grouped_values = grouped.entry(grouped_labels).or_default();
        for sample in values {
            grouped_values.entry(sample.0).or_default().merge(sample.1);
        }
    }

    grouped
}

pub(crate) fn select_all_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut selected = BTreeMap::new();
    for sample_entry in samples {
        let labels = sample_entry.0;
        let mut values_at_time = BTreeMap::new();
        for value_entry in sample_entry.1 {
            let mut value = value_entry.1;
            values_at_time.insert(value_entry.0, range_sample_value(&mut value, query));
        }
        selected.insert(labels, values_at_time);
    }
    selected
}

pub(crate) fn sort_formatted_vector_samples(
    series: &mut FormattedMetricSeries,
    op: &VectorAggregationOp,
) {
    match op {
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            series.sort_by(|left, right| {
                let left_value = left
                    .values
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let right_value = right
                    .values
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let value_order = match op {
                    VectorAggregationOp::Sort => left_value.cmp_value(right_value),
                    VectorAggregationOp::SortDesc => right_value.cmp_value(left_value),
                    _ => Ordering::Equal,
                };
                value_order.then_with(|| left.labels.cmp(&right.labels))
            });
        }
        _ => {}
    }
}

pub(crate) fn vector_group_labels(labels: &Labels, grouping: Option<&VectorGrouping>) -> Labels {
    match grouping {
        Some(VectorGrouping::By(names)) => {
            let mut grouped = Labels::new();
            for name in names {
                if let Some(value) = labels.get(name) {
                    grouped.insert(name.clone(), value.clone());
                }
            }
            grouped
        }
        Some(VectorGrouping::Without(names)) => {
            let mut grouped = Labels::new();
            for entry in labels {
                if !names.contains(entry.0) {
                    grouped.insert(entry.0.clone(), entry.1.clone());
                }
            }
            grouped
        }
        None => Labels::new(),
    }
}

pub(crate) fn aggregate_vector_samples(
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

    for sample_set in samples {
        let grouped_labels =
            vector_group_labels(&sample_set.0, vector_aggregation.grouping.as_ref());
        for sample in sample_set.1 {
            let time = sample.0;
            let mut value = sample.1;
            states
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .record(range_sample_value(&mut value, query));
        }
    }

    let mut output = BTreeMap::new();
    for label_states in states {
        let mut samples = BTreeMap::new();
        for state in label_states.1 {
            samples.insert(state.0, state.1.finish(&vector_aggregation.op));
        }
        output.insert(label_states.0, samples);
    }
    output
}

pub(crate) fn apply_absent_over_time(
    samples: &mut MetricSamples,
    query: &MetricQuery,
    eval_times: &[i64],
) {
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

pub(crate) fn count_values_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    value_label: &str,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut counted: BTreeMap<Labels, BTreeMap<i64, u64>> = BTreeMap::new();

    for sample_set in samples {
        let grouped_labels = vector_group_labels(&sample_set.0, grouping);
        for sample in sample_set.1 {
            let time = sample.0;
            let mut sample_value = sample.1;
            let value = range_sample_value(&mut sample_value, query);
            let mut output_labels = grouped_labels.clone();
            output_labels.insert(value_label.to_string(), format_metric_value(value));
            *counted
                .entry(output_labels)
                .or_default()
                .entry(time)
                .or_default() += 1;
        }
    }

    let mut output = BTreeMap::new();
    for label_counts in counted {
        let mut samples = BTreeMap::new();
        for count in label_counts.1 {
            samples.insert(count.0, MetricValue::integer(count.1));
        }
        output.insert(label_counts.0, samples);
    }
    output
}

pub(crate) fn format_metric_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> FormattedMetricSeries {
    let samples = if let Some(grouping) = &query.range_grouping {
        group_range_samples(samples, grouping)
    } else {
        samples
    };

    if let Some(vector_aggregation) = &query.vector_aggregation {
        let aggregated = aggregate_vector_samples(samples, query, vector_aggregation);
        let mut series = aggregated
            .keys()
            .map(|labels| {
                let values = &aggregated[labels];
                FormattedMetricSeriesEntry {
                    labels: labels.clone(),
                    values: values
                        .keys()
                        .map(|time| [time.to_string(), format_metric_value(values[time])])
                        .collect(),
                }
            })
            .collect::<FormattedMetricSeries>();
        sort_formatted_vector_samples(&mut series, &vector_aggregation.op);
        return series;
    }

    samples
        .into_iter()
        .map(|(labels, values)| FormattedMetricSeriesEntry {
            labels,
            values: values
                .into_iter()
                .map(|(time, mut value)| {
                    [
                        time.to_string(),
                        format_metric_value(range_sample_value(&mut value, query)),
                    ]
                })
                .collect(),
        })
        .collect()
}

pub(crate) struct FormattedMetricSeriesEntry {
    pub(crate) labels: Labels,
    pub(crate) values: Vec<[String; 2]>,
}

pub(crate) type FormattedMetricSeries = Vec<FormattedMetricSeriesEntry>;

pub(crate) type MetricSamples = BTreeMap<Labels, BTreeMap<i64, MetricSampleState>>;

pub(crate) fn metric_samples_from_batches(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_times: &[i64],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<MetricSamples, QueryError> {
    let mut samples: MetricSamples = BTreeMap::new();

    for_each_query_row(batches, &plan.fingerprints, |row| {
        append_matching_metric_row(
            &mut samples,
            plan,
            label_index,
            row,
            MetricWindow {
                query,
                eval_times,
                range_ns: query.range_ns.0,
                delete_filters,
            },
        )
    })?;

    Ok(samples)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MetricValue {
    pub(crate) numerator: i128,
    pub(crate) denominator: u128,
}

#[derive(Clone, Copy)]
pub(crate) struct MetricWindow<'a> {
    pub(crate) query: &'a MetricQuery,
    /// Sorted in ascending order so each row's containing windows form a slice.
    pub(crate) eval_times: &'a [i64],
    pub(crate) range_ns: i64,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
}

pub(crate) fn range_sample_value(
    value: &mut MetricSampleState,
    query: &MetricQuery,
) -> MetricValue {
    match query.aggregation {
        RangeAggregation::CountOverTime
        | RangeAggregation::BytesOverTime
        | RangeAggregation::AbsentOverTime
        | RangeAggregation::SumOverTime => value.summary.sum,
        RangeAggregation::PresentOverTime => MetricValue::integer(1),
        RangeAggregation::Rate | RangeAggregation::BytesRate => {
            rate_metric_value(value.summary.sum, query.range_ns.0)
        }
        RangeAggregation::RateCounter => {
            rate_metric_value(value.counter_increase(), query.range_ns.0)
        }
        RangeAggregation::AvgOverTime => value.average(),
        RangeAggregation::StdvarOverTime => value.stdvar(),
        RangeAggregation::StddevOverTime => value.stddev(),
        RangeAggregation::QuantileOverTime(quantile) => value.quantile(quantile),
        RangeAggregation::MinOverTime => value.summary.min.unwrap_or_else(MetricValue::zero),
        RangeAggregation::MaxOverTime => value.summary.max.unwrap_or_else(MetricValue::zero),
        RangeAggregation::FirstOverTime => value
            .first
            .map_or_else(MetricValue::zero, |sample| sample.value),
        RangeAggregation::LastOverTime => value
            .last
            .map_or_else(MetricValue::zero, |sample| sample.value),
    }
}

struct VectorSampleCandidate {
    labels: Labels,
    value: MetricValue,
}

pub(crate) fn select_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    limit: u64,
    selection: VectorSelection,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut groups: BTreeMap<Labels, BTreeMap<i64, Vec<VectorSampleCandidate>>> = BTreeMap::new();

    for sample_entry in samples {
        let labels = sample_entry.0;
        let values = sample_entry.1;
        let grouped_labels = vector_group_labels(&labels, grouping);
        for value_entry in values {
            let time = value_entry.0;
            let mut value = value_entry.1;
            groups
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .push(VectorSampleCandidate {
                    labels: labels.clone(),
                    value: range_sample_value(&mut value, query),
                });
        }
    }

    let mut selected = BTreeMap::new();
    for group_entry in groups {
        for candidate_entry in group_entry.1 {
            let time = candidate_entry.0;
            let mut candidates = candidate_entry.1;
            candidates.sort_by(|left, right| {
                let value_order = match selection {
                    VectorSelection::Largest => right.value.cmp_value(left.value),
                    VectorSelection::Smallest => left.value.cmp_value(right.value),
                };
                value_order.then_with(|| left.labels.cmp(&right.labels))
            });
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            for candidate in candidates.into_iter().take(limit) {
                selected
                    .entry(candidate.labels)
                    .or_insert_with(BTreeMap::new)
                    .insert(time, candidate.value);
            }
        }
    }

    selected
}

#[derive(Clone, Copy)]
pub(crate) enum VectorSelection {
    Largest,
    Smallest,
}
