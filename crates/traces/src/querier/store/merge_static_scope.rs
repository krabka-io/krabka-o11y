use super::{BTreeMap, BTreeSet, TagScope, TagScopeTags, tag_scope_key};

pub(crate) fn merge_static_scope(
    by_scope: &mut BTreeMap<&'static str, TagScopeTags>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: &[&str],
) {
    if requested.is_some_and(|requested| requested != scope) {
        return;
    }
    let scope_tags = by_scope
        .entry(tag_scope_key(scope))
        .or_insert_with(|| TagScopeTags {
            scope,
            tags: BTreeSet::new(),
        });
    scope_tags
        .tags
        .extend(tags.iter().map(|tag| (*tag).to_string()));
}
