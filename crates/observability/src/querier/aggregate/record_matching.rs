use crate::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, LabelIndex, Labels, MapArray, MetricQuery,
    MetricSamples, MetricValue, MetricWindow, PipelineStage, QueryError, RangeAggregation,
    SeriesFingerprint, StreamPlan, StreamQuery, StringArray, UNWRAP_SAMPLE_VALUE_LABEL,
    WalLogRecord, is_unwrapped_metric_query,
};
pub(crate) fn append_matching_hot_log_record(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) {
    if record.tenant != plan.tenant
        || frontier.is_compacted(record)
        || record.timestamp_ns < plan.time_range.start_ns
        || record.timestamp_ns > plan.time_range.end_ns
    {
        return;
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return;
    }

    if let Some((stream_labels, current_line)) = matching_loki_stream_entry(
        &plan.query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        streams
            .entry(stream_labels)
            .or_default()
            .push([record.timestamp_ns.to_string(), current_line]);
    }
}

pub(crate) fn is_deleted_log_entry(
    delete_filters: &[ActiveLogDeleteFilter],
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> bool {
    delete_filters.iter().any(|filter| {
        timestamp_ns >= filter.time_range.start_ns
            && timestamp_ns <= filter.time_range.end_ns
            && filter
                .query
                .matches_with_fields(labels, line, structured_metadata)
    })
}

pub(crate) fn matching_loki_stream_entry(
    query: &StreamQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Option<(Labels, String)> {
    let evaluation =
        query.evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns)?;
    let mut stream_labels = evaluation.fields;
    stream_labels.remove(UNWRAP_SAMPLE_VALUE_LABEL);
    if should_insert_unknown_detected_level_for_stream_query(query, &stream_labels) {
        stream_labels.insert("detected_level".to_string(), "unknown".to_string());
    }
    Some((stream_labels, evaluation.line))
}

pub(crate) fn matching_loki_metric_sample(
    query: &MetricQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Result<Option<(Labels, String, Option<MetricValue>)>, QueryError> {
    let evaluation =
        query
            .stream
            .evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns);
    let Some(evaluation) = evaluation else {
        return Ok(None);
    };
    if let Some(error) = evaluation
        .fields
        .get("__error__")
        .filter(|error| !error.is_empty())
    {
        return Err(QueryError::MetricPipelineError {
            error: error.clone(),
            details: evaluation.fields.get("__error_details__").cloned(),
        });
    }
    let mut metric_labels = evaluation.fields;
    let unwrap_sample = metric_labels
        .remove(UNWRAP_SAMPLE_VALUE_LABEL)
        .and_then(|value| parse_metric_sample_value(&value));
    for stage in &query.stream.pipeline {
        if let PipelineStage::Unwrap(unwrap) = stage {
            metric_labels.remove(unwrap.label());
        }
    }
    if should_insert_unknown_detected_level_for_stream_query(&query.stream, &metric_labels) {
        metric_labels.insert("detected_level".to_string(), "unknown".to_string());
    }
    Ok(Some((metric_labels, evaluation.line, unwrap_sample)))
}

pub(crate) fn parse_metric_sample_value(value: &str) -> Option<MetricValue> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    Some(MetricValue::new(numerator, denominator))
}

pub(crate) fn parse_decimal_sample_literal(value: &str) -> Option<(i128, u128)> {
    if value.is_empty() {
        return None;
    }
    let (negative, value) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if value.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent_text = &value[index + 1..];
            if exponent_text.find(['e', 'E']).is_some() {
                return None;
            }
            (
                &value[..index],
                parse_decimal_sample_exponent(exponent_text)?,
            )
        }
        None => (value, 0),
    };
    if mantissa.is_empty() {
        return None;
    }

    let (whole, fractional) = match mantissa.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (mantissa, ""),
    };
    if whole.is_empty() && fractional.is_empty() {
        return None;
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(whole.len() + fractional.len());
    digits.push_str(whole);
    digits.push_str(fractional);
    if digits.is_empty() {
        return None;
    }
    let mut numerator = digits.parse::<u128>().ok()?;

    let decimal_places = i64::try_from(fractional.len())
        .ok()?
        .checked_sub(i64::from(exponent))?;
    let denominator = if decimal_places >= 0 {
        10_u128.checked_pow(u32::try_from(decimal_places).ok()?)?
    } else {
        numerator =
            numerator.checked_mul(10_u128.checked_pow(u32::try_from(-decimal_places).ok()?)?)?;
        1
    };
    let denominator = i128::try_from(denominator).ok()?;
    let numerator = i128::try_from(numerator).ok()?;
    Some((
        if negative { -numerator } else { numerator },
        u128::try_from(denominator).ok()?,
    ))
}

pub(crate) fn parse_decimal_sample_exponent(value: &str) -> Option<i32> {
    if value.is_empty() {
        return None;
    }
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() {
        return None;
    }
    value.parse::<i32>().ok()
}

pub(crate) fn should_insert_unknown_detected_level(labels: &Labels) -> bool {
    !labels.contains_key("detected_level")
        && !labels.contains_key("level")
        && !labels.contains_key("severity")
        && !labels.contains_key("severity_text")
}

pub(crate) fn should_insert_unknown_detected_level_for_stream_query(
    query: &StreamQuery,
    labels: &Labels,
) -> bool {
    should_insert_unknown_detected_level(labels)
        && !query
            .pipeline
            .iter()
            .any(|stage| matches!(stage, PipelineStage::KeepLabels(_)))
}

pub(crate) fn sort_loki_stream_values(streams: &mut BTreeMap<Labels, Vec<[String; 2]>>) {
    for values in streams.values_mut() {
        values.sort_by_key(|[timestamp, _]| timestamp.parse::<i64>().unwrap_or(i64::MAX));
    }
}

pub(crate) fn structured_metadata_value(
    metadata: &MapArray,
    row: usize,
) -> Result<Labels, QueryError> {
    let entries = metadata.value(row);
    let keys = entries
        .column_by_name("key")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(QueryError::InvalidColumn {
            column: "structured_metadata.key",
            expected: "Utf8",
        })?;
    let values = entries
        .column_by_name("value")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(QueryError::InvalidColumn {
            column: "structured_metadata.value",
            expected: "Utf8",
        })?;

    Ok((0..entries.len())
        .map(|index| {
            (
                keys.value(index).to_string(),
                values.value(index).to_string(),
            )
        })
        .collect())
}

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

pub(crate) fn append_matching_hot_metric_record(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    window: MetricWindow<'_>,
) -> Result<(), QueryError> {
    let MetricWindow {
        query,
        eval_times,
        range_ns,
        delete_filters,
    } = window;
    if record.tenant != plan.tenant || frontier.is_compacted(record) {
        return Ok(());
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return Ok(());
    }

    if let Some((metric_labels, current_line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
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
            if record.timestamp_ns > window_end_ns.saturating_sub(range_ns)
                && record.timestamp_ns <= window_end_ns
            {
                let sample = samples.entry(*eval_time_ns).or_default();
                sample.record(record.timestamp_ns, value);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct QueryRow<'a> {
    pub(crate) fingerprint: SeriesFingerprint,
    pub(crate) timestamp_ns: i64,
    pub(crate) line: &'a str,
    pub(crate) structured_metadata: &'a Labels,
}
use datafusion::arrow::array::Array as _;
