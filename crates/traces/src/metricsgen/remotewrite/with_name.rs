pub(crate) fn with_name(
    name: &str,
    labels: &[krabka_traceql::MetricLabel],
) -> Vec<krabka_traceql::MetricLabel> {
    let mut out = Vec::with_capacity(labels.len() + 1);
    out.push(krabka_traceql::MetricLabel::new("__name__", name));
    out.extend(labels.iter().cloned());
    out.sort();
    out
}
