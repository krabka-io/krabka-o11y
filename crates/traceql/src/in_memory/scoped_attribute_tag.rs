use super::TagScope;

pub(crate) fn scoped_attribute_tag(tag: &str) -> (&str, Option<TagScope>) {
    match tag.strip_prefix("instrumentation.") {
        Some(tag) => (tag, Some(TagScope::Instrumentation)),
        None => TagScope::split_resource_or_span_prefix(tag),
    }
}
