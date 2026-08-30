use super::pb;

pub(crate) fn label_pairs(labels: Vec<(String, String)>) -> Vec<pb::querier::v1::LabelPair> {
    labels
        .into_iter()
        .map(|(name, value)| pb::querier::v1::LabelPair { name, value })
        .collect()
}
