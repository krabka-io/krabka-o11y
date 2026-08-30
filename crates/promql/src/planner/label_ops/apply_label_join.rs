use super::*;

/// Applies `label_join(v, dst_label, separator, src_label_1, …)` to an
/// already-assembled instant vector.
///
/// For every series, this function sets `dst_label` to the `separator`-joined
/// values of the listed source labels. A missing label contributes the empty
/// string. This mirrors the interpreter's `eval_label_join_call`.
#[must_use]
pub fn apply_label_join(
    samples: Vec<InstantSample>,
    dst_label: &str,
    separator: &str,
    src_labels: &[String],
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .map(|mut sample| {
            let value = src_labels
                .iter()
                .map(|label| sample.labels.get(label).unwrap_or(""))
                .collect::<Vec<_>>()
                .join(separator);
            sample.labels.insert(dst_label, value);
            sample
        })
        .collect()
}
