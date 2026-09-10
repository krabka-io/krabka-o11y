use super::{Labels, bad_bucket_label_warning, emit_warning, parse_classic_bucket_bound};

/// Reads the `le` bound of one classic bucket series.
///
/// A series with no `le`, or with an `le` that is not a float, is not a bucket
/// at all. `resetHistograms` warns and drops it rather than failing the query,
/// because one malformed series should not take the rest of the vector with it.
pub(crate) fn classic_bucket_bound(labels: &Labels) -> Option<f64> {
    let label = labels.get("le").unwrap_or("");
    let Ok(bound) = parse_classic_bucket_bound(label) else {
        emit_warning(bad_bucket_label_warning(
            label,
            labels.get("__name__").unwrap_or(""),
        ));
        return None;
    };
    Some(bound)
}
