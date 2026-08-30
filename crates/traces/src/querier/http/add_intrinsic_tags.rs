use super::{EVENT_TAGS, INSTRUMENTATION_TAGS, INTRINSIC_TAGS, LINK_TAGS, ScopedTag, TagScope, merge_static_scope};

pub(crate) fn add_intrinsic_tags(mut tags: Vec<ScopedTag>, scope: Option<TagScope>) -> Vec<ScopedTag> {
    if matches!(scope, None | Some(TagScope::Intrinsic)) {
        merge_static_scope(&mut tags, TagScope::Intrinsic, INTRINSIC_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Event)) {
        merge_static_scope(&mut tags, TagScope::Event, EVENT_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Link)) {
        merge_static_scope(&mut tags, TagScope::Link, LINK_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Instrumentation)) {
        merge_static_scope(&mut tags, TagScope::Instrumentation, INSTRUMENTATION_TAGS);
    }
    tags
}
