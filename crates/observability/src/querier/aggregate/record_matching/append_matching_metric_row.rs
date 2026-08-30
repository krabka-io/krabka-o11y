use super::*;

pub(crate) fn append_matching_metric_row(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    row: QueryRow<'_>,
    window: MetricWindow<'_>,
) -> Result<(), QueryError> {
    let MetricWindow {
        query,
        eval_times,
        range_ns,
        delete_filters,
    } = window;
    if !plan.fingerprints.contains(&row.fingerprint) {
        return Ok(());
    }

    let labels = label_index
        .labels_for(&plan.tenant, row.fingerprint)
        .ok_or(QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint: row.fingerprint,
        })?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some((metric_labels, current_line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    )? {
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
            if row.timestamp_ns > window_end_ns.saturating_sub(range_ns)
                && row.timestamp_ns <= window_end_ns
            {
                let sample = samples.entry(*eval_time_ns).or_default();
                sample.record(row.timestamp_ns, value);
            }
        }
    }

    Ok(())
}
