use super::*;

pub(crate) fn unscoped_attribute_tag(tag: &str) -> &str {
    tag.strip_prefix("resource.")
        .or_else(|| tag.strip_prefix("span."))
        .or_else(|| tag.strip_prefix("event.").filter(|tag| tag.contains('.')))
        .or_else(|| tag.strip_prefix("link.").filter(|tag| tag.contains('.')))
        .unwrap_or(tag)
}
