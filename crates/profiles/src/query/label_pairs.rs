use super::pb;

pub(crate) fn label_pairs(
    labels: Vec<krabka_pprof::ProfileLabel>,
) -> Vec<pb::querier::v1::LabelPair> {
    labels
        .into_iter()
        .map(|label| pb::querier::v1::LabelPair {
            name: label.name,
            value: label.value,
        })
        .collect()
}

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
