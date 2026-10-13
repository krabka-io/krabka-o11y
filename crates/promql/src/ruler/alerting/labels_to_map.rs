use super::BTreeMap;

pub(crate) fn labels_to_map(labels: &crate::PromqlLabels) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for pair in labels.iter() {
        values.insert(pair.name.clone(), pair.value.as_str().to_owned());
    }
    values
}
