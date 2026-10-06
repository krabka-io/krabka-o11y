use super::BTreeMap;

pub(crate) fn labels_to_map(labels: &crate::PromqlLabels) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), value.as_str().to_owned()))
        .collect()
}
