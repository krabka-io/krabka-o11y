use super::{InstantSample, set_label_value};

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
    samples
        .into_iter()
        .map(|mut sample| {
            let mut value = Vec::new();
            for (index, name) in src_labels.iter().enumerate() {
                if index > 0 {
                    value.extend_from_slice(separator.as_bytes());
                }
                if let Some(source) = sample.labels.get_value(name) {
                    value.extend_from_slice(source.as_bytes());
                }
            }
            sample.labels =
                set_label_value(&sample.labels, dst_label, crate::PromqlString::from(value));
            if dst_label == "__name__" {
                sample.drop_name = false;
            }
            sample
        })
        .collect()
}
