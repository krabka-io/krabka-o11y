use super::Labels;

/// Writes `value` to `name`, or removes `name` when `value` is empty.
///
/// `label_replace` and `label_join` both write their result through
/// `labels.Builder.Set`, and that method deletes the label when the value is
/// empty, because Prometheus holds an empty label and a missing one to be the
/// same thing.
pub(crate) fn set_label_value(labels: &Labels, name: &str, value: &str) -> Labels {
    if value.is_empty() {
        return labels
            .iter()
            .filter(|(label, _)| label.as_str() != name)
            .map(|(label, value)| (label.clone(), value.clone()))
            .collect();
    }
    let mut labels = labels.clone();
    labels.insert(name, value);
    labels
}
