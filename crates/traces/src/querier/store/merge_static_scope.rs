use super::{BTreeMap, BTreeSet, TagScope, tag_scope_key};

pub(crate) fn merge_static_scope(
    by_scope: &mut BTreeMap<&'static str, (TagScope, BTreeSet<String>)>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: &[&str],
) {
    if requested.is_some_and(|requested| requested != scope) {
        return;
    }
    let (_, out) = by_scope
        .entry(tag_scope_key(scope))
        .or_insert((scope, BTreeSet::new()));
    out.extend(tags.iter().map(|tag| (*tag).to_string()));
}
