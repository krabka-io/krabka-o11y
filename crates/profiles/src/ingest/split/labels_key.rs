use super::Labels;

pub(crate) fn labels_key(labels: &Labels) -> Vec<krabka_blockstore::LabelPair> {
    labels
        .iter()
        .map(|label| krabka_blockstore::LabelPair::new(label.name, label.value))
        .collect()
}
