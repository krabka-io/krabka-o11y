use super::*;

pub(crate) fn parse_tag_scope(scope: &str) -> Option<TagScope> {
    match scope {
        "resource" => Some(TagScope::Resource),
        "span" => Some(TagScope::Span),
        "intrinsic" => Some(TagScope::Intrinsic),
        "event" => Some(TagScope::Event),
        "link" => Some(TagScope::Link),
        "instrumentation" => Some(TagScope::Instrumentation),
        _ => None,
    }
}
