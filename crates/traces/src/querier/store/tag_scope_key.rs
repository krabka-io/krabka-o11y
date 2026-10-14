use super::TagScope;

pub(crate) fn tag_scope_key(scope: TagScope) -> &'static str {
    crate::frontend::merge::scope_key(scope)
}
