
pub(crate) fn scoped_attribute_tag(tag: &str) -> (&str, Option<krabka_traceql::TagScope>) {
    if let Some(tag) = tag.strip_prefix("resource.") {
        (tag, Some(krabka_traceql::TagScope::Resource))
    } else if let Some(tag) = tag.strip_prefix("span.") {
        (tag, Some(krabka_traceql::TagScope::Span))
    } else {
        (tag, None)
    }
}
