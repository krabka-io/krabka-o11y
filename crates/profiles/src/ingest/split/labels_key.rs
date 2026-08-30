use super::*;

pub(crate) fn labels_key(labels: &Labels) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
