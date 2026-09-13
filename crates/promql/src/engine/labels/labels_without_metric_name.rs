use super::Labels;

pub(crate) fn labels_without_metric_name(input: &Labels) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        // `__name__` is removed only when each operator's Prometheus semantics
        // require it; classic histogram grouping still needs the source name.
        if !matches!(name.as_str(), "__type__" | "__unit__") {
            labels.insert(name, value);
        }
    }
    labels
}
