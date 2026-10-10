use super::*;

/// An entry that matched a metric query.
pub(crate) struct MatchedEntry {
    pub(crate) timestamp_ns: i64,
    /// The labels of the series its sample belongs to.
    pub(crate) metric_labels: Labels,
    pub(crate) line: String,
    /// The value an `unwrap` stage extracted, if the query has one.
    pub(crate) unwrap_sample: Option<MetricValue>,
}

/// Records `entry` as the sample of its metric labels in every evaluation
/// window of `window` that the entry's timestamp falls in.
///
/// The sample is the entry's count, its line length, or its unwrapped value,
/// as the query's range aggregation asks.
pub(crate) fn record_matching_metric_sample(
    samples: &mut MetricSamples,
    window: MetricWindow<'_>,
    entry: MatchedEntry,
) {
    let MetricWindow {
        query,
        eval_times,
        range_ns,
        ..
    } = window;
    let MatchedEntry {
        timestamp_ns,
        metric_labels,
        line: current_line,
        unwrap_sample,
    } = entry;
    let samples = samples.entry(metric_labels).or_default();
    let is_unwrapped = is_unwrapped_metric_query(query);
    let value = match query.aggregation {
        RangeAggregation::Rate if is_unwrapped => unwrap_sample.unwrap_or_default(),
        RangeAggregation::CountOverTime
        | RangeAggregation::Rate
        | RangeAggregation::AbsentOverTime
        | RangeAggregation::PresentOverTime => MetricValue::integer(1),
        RangeAggregation::BytesRate | RangeAggregation::BytesOverTime => {
            MetricValue::integer(current_line.len() as u64)
        }
        RangeAggregation::RateCounter
        | RangeAggregation::SumOverTime
        | RangeAggregation::AvgOverTime
        | RangeAggregation::StdvarOverTime
        | RangeAggregation::StddevOverTime
        | RangeAggregation::QuantileOverTime(_)
        | RangeAggregation::MinOverTime
        | RangeAggregation::MaxOverTime
        | RangeAggregation::FirstOverTime
        | RangeAggregation::LastOverTime => unwrap_sample.unwrap_or_default(),
    };
    for eval_time_ns in eval_times {
        let window_end_ns = eval_time_ns.saturating_sub(query.offset_ns.0);
        if timestamp_ns > window_end_ns.saturating_sub(range_ns) && timestamp_ns <= window_end_ns {
            let sample = samples.entry(*eval_time_ns).or_default();
            sample.record(timestamp_ns, value);
        }
    }
}
