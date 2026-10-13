use super::{
    BTreeMap, Labels, QueryResult, RecordBatch, Result, SeriesFingerprint,
    assemble_float_vector_batches::assemble_float_vector_batches as assemble_values,
    over_time_range,
};

/// Assembles `*_over_time` projection output batches into a result.
///
/// `preserve_metric_name` keeps `__name__` only for `last_over_time`. Empty
/// windows are omitted while genuine NaN results are retained.
pub(crate) fn assemble_over_time_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
    preserve_metric_name: bool,
) -> Result<QueryResult> {
    assemble_values(
        batches,
        labels_by_fp,
        time_ms,
        over_time_range::OVER_TIME_VALUE_COLUMN,
        "over_time projection missing Float64 value column",
        !preserve_metric_name,
    )
}
