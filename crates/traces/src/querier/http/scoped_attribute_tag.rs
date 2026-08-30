use super::*;

pub(crate) fn scoped_attribute_tag(tag: &str) -> (&str, Option<TagScope>) {
    if let Some(tag) = tag.strip_prefix("resource.") {
        (tag, Some(TagScope::Resource))
    } else if let Some(tag) = tag.strip_prefix("span.") {
        (tag, Some(TagScope::Span))
    } else {
        (tag, None)
    }
}
