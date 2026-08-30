
pub(crate) fn span_kind_json(kind: i32) -> Option<&'static str> {
    match kind {
        1 => Some("SPAN_KIND_INTERNAL"),
        2 => Some("SPAN_KIND_SERVER"),
        3 => Some("SPAN_KIND_CLIENT"),
        4 => Some("SPAN_KIND_PRODUCER"),
        5 => Some("SPAN_KIND_CONSUMER"),
        _ => None,
    }
}
