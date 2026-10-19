use datafusion::arrow::array::{Array as _, Int64Array, UInt64Array};

use crate::{
    ActiveLogDeleteFilter, BTreeMap, BTreeSet, CompactionFrontier, LabelFormatAssignment,
    LabelIndex, Labels, LokiStreamEntry, MapArray, MetricQuery, MetricSamples, MetricValue,
    MetricWindow, PipelineStage, QueryError, RangeAggregation, RecordBatch, SeriesFingerprint,
    StreamPlan, StreamQuery, StringArray, UNWRAP_SAMPLE_VALUE_LABEL, WalLogRecord,
    is_unwrapped_metric_query,
};

mod append_matching_hot_log_record;
mod apply_distinct_to_streams;
mod query_row;

#[cfg(test)]
mod tests;

pub(crate) use append_matching_hot_log_record::append_matching_hot_log_record;
pub(crate) use apply_distinct_to_streams::apply_distinct_to_streams;
pub(crate) use krabka_logql::parse_decimal_sample_literal;
pub(crate) use query_row::QueryRow;

pub(crate) fn parse_metric_sample_value(value: &str) -> Option<MetricValue> {
    let ratio = parse_decimal_sample_literal(value)?;
    Some(MetricValue::new(ratio.numerator, ratio.denominator))
}

pub(crate) fn sort_loki_stream_values(streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>) {
    for values in streams.values_mut() {
        values.sort_by_cached_key(|entry| entry.parsed_timestamp_ns().unwrap_or(i64::MAX));
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

/// The label names a `label_format` stage of `query` writes.
///
/// Loki categorises such a label as parsed whatever it held before -- a series
/// label, a piece of structured metadata, or nothing at all -- and it does so
/// even when the value written is the one the label already carried. Krabka's
/// pipeline hands back one flat field map, in which a rewrite to the same
/// string is indistinguishable from no rewrite, so the query is asked instead
/// of the value.
pub(crate) fn label_format_destinations(query: &StreamQuery) -> BTreeSet<&str> {
    query
        .pipeline
        .iter()
        .filter_map(|stage| match stage {
            PipelineStage::LabelFormat(format) => Some(format.assignments()),
            _ => None,
        })
        .flatten()
        .map(LabelFormatAssignment::destination)
        .collect()
}

pub(crate) fn structured_metadata_value(
    metadata: &MapArray,
    row: usize,
) -> Result<Labels, QueryError> {
    let entries =
        krabka_blockstore::map_array_string_entries(metadata, row, |column, expected| {
            QueryError::InvalidColumn { column, expected }
        })?;
    let mut labels = Labels::new();
    for entry in entries {
        labels.insert(entry.key, entry.value);
    }
    Ok(labels)
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
        delete_filters,
        ..
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

    if let Some(sample) = matching_loki_metric_sample(
        query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    )? {
        record_matching_metric_sample(
            samples,
            window,
            MatchedEntry {
                timestamp_ns: record.timestamp_ns,
                metric_labels: sample.labels,
                line: sample.line,
                unwrap_sample: sample.unwrap_sample,
            },
        );
    }
    Ok(())
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
        delete_filters,
        ..
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
    if let Some(sample) = matching_loki_metric_sample(
        query,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    )? {
        record_matching_metric_sample(
            samples,
            window,
            MatchedEntry {
                timestamp_ns: row.timestamp_ns,
                metric_labels: sample.labels,
                line: sample.line,
                unwrap_sample: sample.unwrap_sample,
            },
        );
    }

    Ok(())
}

/// Calls `visit` with every row of `batches` whose series is in `series`.
/// The batches hold the columns a log block scan selects: the series
/// fingerprint, the timestamp, the line, and the structured metadata.
pub(crate) fn for_each_query_row(
    batches: &[RecordBatch],
    series: &BTreeSet<SeriesFingerprint>,
    mut visit: impl FnMut(QueryRow<'_>) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
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

        let mut previous_fingerprint = None;
        let mut fingerprint_matches = false;
        for row in 0..batch.num_rows() {
            let fingerprint = fingerprints.value(row);
            if previous_fingerprint != Some(fingerprint) {
                fingerprint_matches = series.contains(&fingerprint);
                previous_fingerprint = Some(fingerprint);
            }
            // A large SQL selection is a coarse range. Reject its extra rows
            // before allocating their structured metadata. Ordered rows share
            // this lookup; a nonadjacent fingerprint is checked again.
            if !fingerprint_matches {
                continue;
            }
            let structured_metadata = structured_metadata_value(metadata, row)?;
            visit(QueryRow {
                fingerprint,
                timestamp_ns: timestamps.value(row),
                line: lines.value(row),
                structured_metadata: &structured_metadata,
            })?;
        }
    }
    Ok(())
}

pub(crate) struct MatchingMetricSample {
    pub(crate) labels: Labels,
    pub(crate) line: String,
    pub(crate) unwrap_sample: Option<MetricValue>,
}

pub(crate) fn matching_loki_metric_sample(
    query: &MetricQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Result<Option<MatchingMetricSample>, QueryError> {
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
    Ok(Some(MatchingMetricSample {
        labels: metric_labels,
        line: evaluation.line,
        unwrap_sample,
    }))
}

pub(crate) struct MatchingStreamEntry {
    pub(crate) labels: Labels,
    pub(crate) entry: LokiStreamEntry,
}

/// Evaluates one record against a stream query, and buckets what comes out.
///
/// The returned labels are the stream's under Loki's default encoding: the
/// series' own labels, plus the record's structured metadata, plus whatever a
/// parser or `label_format` stage produced. Two records that differ only by a
/// metadata value therefore land in two streams, which is what Loki returns.
/// The entry keeps the same labels bucketed by where they came from, so that a
/// `categorize-labels` response can lift them back out.
///
/// The bucketing reads a label's origin off its value, because the pipeline
/// hands back one flat field map rather than Loki's per-label category. Two
/// things keep that from mattering. A parser stage cannot silently overwrite a
/// label it did not produce -- a name already taken is written as
/// `<name>_extracted` -- so the only stage whose write can hide behind an
/// unchanged value is `label_format`, and the query names the labels that
/// stage writes even when the evaluated fields cannot.
pub(crate) fn matching_loki_stream_entry(
    query: &StreamQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
    preserve_source_labels: bool,
) -> Option<MatchingStreamEntry> {
    let evaluation =
        query.evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns)?;
    let mut stream_labels = evaluation.fields;
    stream_labels.remove(UNWRAP_SAMPLE_VALUE_LABEL);

    // A `label_format` destination is parsed whatever it held before, and
    // Loki's own answer moves it out of `structuredMetadata` as readily as out
    // of the stream, so this is asked before either.
    let formatted = label_format_destinations(query);
    let mut entry_metadata = Labels::new();
    let mut parsed = Labels::new();
    for label in &stream_labels {
        let name = label.0;
        let value = label.1;
        if formatted.contains(name.as_str()) {
            parsed.insert(name.clone(), value.clone());
            continue;
        }
        if labels.get(name).is_none() && structured_metadata.get(name) == Some(value) {
            entry_metadata.insert(name.clone(), value.clone());
        } else if labels.get(name) != Some(value) {
            // A value the series did not carry and the metadata did not carry
            // is one a pipeline stage produced, including a stage that
            // overwrote either.
            parsed.insert(name.clone(), value.clone());
        }
    }

    let mut entry = LokiStreamEntry::new(timestamp_ns, evaluation.line, entry_metadata, parsed);
    // Distinct uses the original source even when the pipeline changes labels.
    // Tail callers also need the source for live-frame grouping.
    if preserve_source_labels
        || query
            .pipeline
            .iter()
            .any(|stage| matches!(stage, krabka_logql::PipelineStage::Distinct(_)))
    {
        entry.source_labels = labels.clone();
    }
    Some(MatchingStreamEntry {
        labels: stream_labels,
        entry,
    })
}

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
    // Both boundaries are monotone on the sorted evaluation grid, including
    // where subtraction saturates. Search the original boundary expressions:
    // moving the offset or range to the other side changes extreme timestamps.
    let first =
        eval_times.partition_point(|time| time.saturating_sub(query.offset_ns.0) < timestamp_ns);
    let remaining = &eval_times[first..];
    let count = remaining.partition_point(|time| {
        time.saturating_sub(query.offset_ns.0)
            .saturating_sub(range_ns)
            < timestamp_ns
    });
    for eval_time_ns in &remaining[..count] {
        let sample = samples.entry(*eval_time_ns).or_default();
        sample.record(timestamp_ns, value);
    }
}

pub(crate) fn append_matching_log_row(
    streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    row: QueryRow<'_>,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    let QueryRow {
        fingerprint,
        timestamp_ns,
        line,
        structured_metadata,
    } = row;
    if timestamp_ns < plan.time_range.start_ns
        || timestamp_ns > plan.time_range.end_ns
        || !plan.fingerprints.contains(&fingerprint)
    {
        return Ok(());
    }

    let labels = label_index.labels_for(&plan.tenant, fingerprint).ok_or(
        QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint,
        },
    )?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        line,
        structured_metadata,
        timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some(matched) = matching_loki_stream_entry(
        &plan.query,
        labels,
        line,
        structured_metadata,
        timestamp_ns,
        false,
    ) {
        streams
            .entry(matched.labels)
            .or_default()
            .push(matched.entry);
    }

    Ok(())
}
