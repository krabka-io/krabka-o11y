use super::*;

pub(crate) fn label_key_for_span(
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
) -> LabelKey {
    if is_client {
        (span.service_name.clone(), String::new(), connection_type)
    } else {
        (String::new(), span.service_name.clone(), connection_type)
    }
}
