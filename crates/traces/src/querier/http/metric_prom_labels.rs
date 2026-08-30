/// Prometheus-style label string for `TimeSeries.promLabels`, which is
/// Grafana's legend. An example is `{resource_service_name="api"}`. An empty
/// label set renders as `{}`.
pub(crate) fn metric_prom_labels(labels: &[(String, String)]) -> String {
    let inner = labels
        .iter()
        .map(|(key, value)| {
            format!(
                "{}=\"{}\"",
                key.replace('.', "_"),
                value.replace('"', "\\\"")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{inner}}}")
}
