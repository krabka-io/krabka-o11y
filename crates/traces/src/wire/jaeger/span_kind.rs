use super::*;

pub(crate) fn span_kind(tags: &[KeyValue]) -> SpanKind {
    tags.iter()
        .find_map(|tag| {
            if tag.key != "span.kind" {
                return None;
            }
            match &tag.value {
                AttrValue::Str(value) => match value.as_str() {
                    "server" => Some(SpanKind::Server),
                    "client" => Some(SpanKind::Client),
                    "producer" => Some(SpanKind::Producer),
                    "consumer" => Some(SpanKind::Consumer),
                    "internal" => Some(SpanKind::Internal),
                    _ => None,
                },
                _ => None,
            }
        })
        .unwrap_or(SpanKind::Internal)
}
