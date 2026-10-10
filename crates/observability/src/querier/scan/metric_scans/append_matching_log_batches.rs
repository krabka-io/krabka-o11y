use super::{
    ActiveLogDeleteFilter, BTreeMap, Int64Array, LabelIndex, Labels, LokiStreamEntry, MapArray,
    QueryError, QueryRow, RecordBatch, StreamPlan, StringArray, UInt64Array,
    append_matching_log_row, structured_metadata_value,
};

pub(crate) fn append_matching_log_batches(
    streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    batches: &[RecordBatch],
    delete_filters: &[ActiveLogDeleteFilter],
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
                fingerprint_matches = plan.fingerprints.contains(&fingerprint);
                previous_fingerprint = Some(fingerprint);
            }
            // A large SQL selection is a coarse range. Reject its extra rows
            // before allocating their structured metadata. Ordered rows share
            // this lookup; a nonadjacent fingerprint is checked again.
            if !fingerprint_matches {
                continue;
            }
            let structured_metadata = structured_metadata_value(metadata, row)?;
            append_matching_log_row(
                streams,
                plan,
                label_index,
                QueryRow {
                    fingerprint,
                    timestamp_ns: timestamps.value(row),
                    line: lines.value(row),
                    structured_metadata: &structured_metadata,
                },
                delete_filters,
            )?;
        }
    }
    Ok(())
}
