use super::TagScope;

pub(crate) fn tag_scope_key(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}
