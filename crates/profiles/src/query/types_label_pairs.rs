use super::pb;

pub(crate) fn types_label_pairs(labels: Vec<(String, String)>) -> Vec<pb::types::v1::LabelPair> {
    labels
        .into_iter()
        .map(|(name, value)| pb::types::v1::LabelPair { name, value })
        .collect()
}
