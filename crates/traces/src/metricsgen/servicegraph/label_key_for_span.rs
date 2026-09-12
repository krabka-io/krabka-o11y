use super::{ConnectionType, LabelKey, SpanRecord};

pub(crate) fn label_key_for_span(
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
    labels: Vec<(String, String)>,
) -> LabelKey {
    if is_client {
        (
            span.service_name.clone(),
            String::new(),
            connection_type,
            labels,
        )
    } else {
        (
            String::new(),
            span.service_name.clone(),
            connection_type,
            labels,
        )
    }
}
