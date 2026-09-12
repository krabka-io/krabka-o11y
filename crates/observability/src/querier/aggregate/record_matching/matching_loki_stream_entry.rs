use super::{
    Labels, LokiStreamEntry, StreamQuery, UNWRAP_SAMPLE_VALUE_LABEL, label_format_destinations,
    should_insert_unknown_detected_level_for_stream_query,
};

/// The label Loki's log-level discovery writes, and which it carries as
/// structured metadata rather than as a series label.
const DETECTED_LEVEL_LABEL: &str = "detected_level";

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
) -> Option<(Labels, LokiStreamEntry)> {
    let evaluation =
        query.evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns)?;
    let mut stream_labels = evaluation.fields;
    stream_labels.remove(UNWRAP_SAMPLE_VALUE_LABEL);
    if should_insert_unknown_detected_level_for_stream_query(query, &stream_labels) {
        stream_labels.insert(DETECTED_LEVEL_LABEL.to_string(), "unknown".to_string());
    }

    // A `label_format` destination is parsed whatever it held before, and
    // Loki's own answer moves it out of `structuredMetadata` as readily as out
    // of the stream, so this is asked before either.
    let formatted = label_format_destinations(query);
    let mut entry_metadata = Labels::new();
    let mut parsed = Labels::new();
    for (name, value) in &stream_labels {
        if formatted.contains(name.as_str()) {
            parsed.insert(name.clone(), value.clone());
            continue;
        }
        // Loki's level discovery writes `detected_level` as structured
        // metadata, not as a series label, so it is bucketed with the metadata
        // whether it was discovered at ingest or filled in as `unknown` here.
        let discovered_level =
            name == DETECTED_LEVEL_LABEL && labels.get(name).is_none_or(|label| label == value);
        if discovered_level
            || (!labels.contains_key(name) && structured_metadata.get(name) == Some(value))
        {
            entry_metadata.insert(name.clone(), value.clone());
        } else if labels.get(name) != Some(value) {
            // A value the series did not carry and the metadata did not carry
            // is one a pipeline stage produced, including a stage that
            // overwrote either.
            parsed.insert(name.clone(), value.clone());
        }
    }

    let mut entry = LokiStreamEntry::new(timestamp_ns, evaluation.line, entry_metadata, parsed);
    entry.source_labels = labels.clone();
    Some((stream_labels, entry))
}
