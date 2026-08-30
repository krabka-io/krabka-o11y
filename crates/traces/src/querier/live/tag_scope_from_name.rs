use super::TagScope;

pub(crate) fn tag_scope_from_name(value: &str) -> Option<TagScope> {
    match value {
        "resource" => Some(TagScope::Resource),
        "span" => Some(TagScope::Span),
        "intrinsic" => Some(TagScope::Intrinsic),
        "event" => Some(TagScope::Event),
        "link" => Some(TagScope::Link),
        "instrumentation" => Some(TagScope::Instrumentation),
        _ => None,
    }
}
