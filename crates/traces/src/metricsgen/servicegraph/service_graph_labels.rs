use super::{LabelKey, sorted_labels};

pub(crate) fn service_graph_labels(key: LabelKey) -> Vec<krabka_traceql::MetricLabel> {
    let mut dimensions = key.dimensions;
    dimensions.extend([
        krabka_traceql::MetricLabel::new("client", key.client),
        krabka_traceql::MetricLabel::new("server", key.server),
        krabka_traceql::MetricLabel::new("connection_type", key.connection_type.as_label()),
    ]);
    sorted_labels(dimensions)
}
