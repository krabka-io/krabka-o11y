use crate::result::{TagScope, TagScopePrefix};

pub(crate) fn scoped_attribute_tag(tag: &str) -> TagScopePrefix<'_> {
    match tag.strip_prefix("instrumentation.") {
        Some(tag) => TagScopePrefix {
            tag,
            scope: Some(TagScope::Instrumentation),
        },
        None => TagScope::split_resource_or_span_prefix(tag),
    }
}
