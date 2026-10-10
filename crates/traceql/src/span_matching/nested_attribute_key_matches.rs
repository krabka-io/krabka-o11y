pub(crate) fn nested_attribute_key_matches(key: &str, tag: &str, scope_prefix: &str) -> bool {
    key == tag || tag.strip_prefix(scope_prefix).is_some_and(|tag| key == tag)
}
