use super::pb;

pub(crate) fn types_label_pairs(
    labels: Vec<krabka_pprof::ProfileLabel>,
) -> Vec<pb::types::v1::LabelPair> {
    labels
        .into_iter()
        .map(|label| pb::types::v1::LabelPair {
            name: label.name,
            value: label.value,
        })
        .collect()
}
