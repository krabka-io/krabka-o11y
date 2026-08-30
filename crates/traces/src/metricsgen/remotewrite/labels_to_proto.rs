use super::Label;

pub(crate) fn labels_to_proto(labels: &[(String, String)]) -> Vec<Label> {
    labels
        .iter()
        .map(|(name, value)| Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}
