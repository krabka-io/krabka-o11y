use super::{
    BTreeMap, Labels, QueryResult, RecordBatch, Result, SeriesFingerprint,
    assemble_float_vector_batches::assemble_float_vector_batches as assemble_values, rate_range,
};

/// Assembles rate-family projection output batches into a result.
///
/// Empty windows are omitted while genuine NaN results are retained. Rate
/// families always drop the metric name.
pub(crate) fn assemble_rate_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
) -> Result<QueryResult> {
    assemble_values(
        batches,
        labels_by_fp,
        time_ms,
        rate_range::RATE_VALUE_COLUMN,
        "rate projection missing Float64 value column",
        true,
    )
}
