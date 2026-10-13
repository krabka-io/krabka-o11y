use super::{InstantSample, apply_byte_label_join::apply_join_samples};

/// Applies `label_join(v, dst_label, separator, src_label_1, …)` to an
/// already-assembled instant vector.
///
/// For every series, this function sets `dst_label` to the `separator`-joined
/// values of the listed source labels. A missing label contributes the empty
/// string, and a join that comes out empty REMOVES `dst_label`, because
/// Prometheus writes the result through `labels.Builder.Set`, which deletes a
/// label rather than store it empty.
#[must_use]
pub fn apply_label_join(
    samples: Vec<InstantSample>,
    dst_label: &str,
    separator: &str,
    src_labels: &[String],
) -> Vec<InstantSample> {
    let source_names = src_labels.iter().map(String::as_str).collect::<Vec<_>>();
    apply_join_samples(samples, dst_label, separator.as_bytes(), &source_names)
}
