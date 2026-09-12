use super::Labels;

pub(crate) fn labels_without_metric_name(input: &Labels) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        // `__name__` is kept until the outer query boundary. This lets later
        // label rewrites and aggregations observe it while type/unit metadata
        // is still removed at the operator that changes the sample value.
        if !matches!(name.as_str(), "__type__" | "__unit__") {
            labels.insert(name, value);
        }
    }
    labels
}
