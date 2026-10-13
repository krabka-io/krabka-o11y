use super::Label;

pub(crate) fn labels_to_proto(labels: &[krabka_traceql::MetricLabel]) -> Vec<Label> {
    labels
        .iter()
        .map(|label| Label {
            name: label.key.clone(),
            value: label.value.clone(),
        })
        .collect()
}
