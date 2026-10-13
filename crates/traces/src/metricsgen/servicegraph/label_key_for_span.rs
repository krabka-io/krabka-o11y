use super::{ConnectionType, LabelKey, SpanRecord};

pub(crate) fn label_key_for_span(
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
    labels: Vec<krabka_traceql::MetricLabel>,
) -> LabelKey {
    if is_client {
        LabelKey {
            client: span.service_name.clone(),
            server: String::new(),
            connection_type,
            dimensions: labels,
        }
    } else {
        LabelKey {
            client: String::new(),
            server: span.service_name.clone(),
            connection_type,
            dimensions: labels,
        }
    }
}
