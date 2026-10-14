/// Sort labels into a deterministic order for the encoder and the tests.
#[must_use]
pub fn sorted_labels(
    mut labels: Vec<krabka_traceql::MetricLabel>,
) -> Vec<krabka_traceql::MetricLabel> {
    labels.sort();
    labels
}
