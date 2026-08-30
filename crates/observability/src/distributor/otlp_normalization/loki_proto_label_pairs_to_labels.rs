use super::*;

pub(crate) fn loki_proto_label_pairs_to_labels(labels: &[LokiProtoLabelPair]) -> Labels {
    let mut labels_by_name = Labels::new();
    for label in labels {
        labels_by_name.insert(label.name.clone(), label.value.clone());
    }
    labels_by_name
}
