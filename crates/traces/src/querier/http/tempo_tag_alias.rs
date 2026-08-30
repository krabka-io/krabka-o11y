use super::*;

pub(crate) fn tempo_tag_alias(tag: &str) -> &str {
    match tag.strip_prefix('.').unwrap_or(tag) {
        "name" => "span:name",
        "status" => "span:status",
        tag => tag,
    }
}
