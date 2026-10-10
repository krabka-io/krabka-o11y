use super::{BTreeSet, Labels};

/// The distinct values that label `name` takes across `label_sets`, sorted.
pub(crate) fn label_values_data(label_sets: Vec<Labels>, name: &str) -> Vec<String> {
    let mut values = BTreeSet::new();
    for labels in label_sets {
        if let Some(value) = labels.get(name) {
            values.insert(value.clone());
        }
    }

    values.into_iter().collect::<Vec<_>>()
}
