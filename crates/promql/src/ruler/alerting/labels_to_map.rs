use super::{Labels, BTreeMap};

pub(crate) fn labels_to_map(labels: &Labels) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
