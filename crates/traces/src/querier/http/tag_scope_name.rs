use super::TagScope;

pub(crate) fn tag_scope_name(scope: TagScope) -> &'static str {
    crate::frontend::merge::scope_key(scope)
}
