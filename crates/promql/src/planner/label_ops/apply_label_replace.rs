use super::{InstantSample, Result};

/// Applies `label_replace(v, dst_label, replacement, src_label, regex)` to an
/// already-assembled instant vector.
///
/// `regex` is fully anchored as `^(?:<regex>)$`, as in Prometheus. For each
/// series whose `src_label` value matches `regex` in full, this function sets
/// the destination label to `replacement` with `$1` and `${name}` capture-group
/// expansion. A series that does not match passes through unchanged. This
/// function keeps `__name__` unless `dst_label == "__name__"`; these functions
/// never drop the metric name themselves. An empty expansion REMOVES
/// `dst_label`, because Prometheus writes the result through
/// `labels.Builder.Set`, which deletes a label rather than store it empty. The
/// removal then takes part in later collision checks exactly as Prometheus sees
/// it, which is what makes `label_replace(testmetric, "src", "", "", "")` a
/// duplicate-labelset failure.
///
/// # Errors
///
/// Returns [`PromqlError::Exec`] when `regex` is not a valid regular expression.
/// The error text matches the interpreter's error text.
pub fn apply_label_replace(
    samples: Vec<InstantSample>,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex: &str,
) -> Result<Vec<InstantSample>> {
    super::apply_byte_label_replace(
        samples,
        &dst_label.into(),
        &replacement.into(),
        &src_label.into(),
        &regex.into(),
    )
}
