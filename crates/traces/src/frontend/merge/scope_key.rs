use super::TagScope;

/// Stable string discriminant for a `TagScope`. It is the ordering key and the
/// dedup key.
pub(crate) fn scope_key(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}
