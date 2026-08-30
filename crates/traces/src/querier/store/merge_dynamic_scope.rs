use super::{Array, BTreeMap, BTreeSet, TagScope, tag_scope_key};

pub(crate) fn merge_dynamic_scope(
    by_scope: &mut BTreeMap<&'static str, (TagScope, BTreeSet<String>)>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: BTreeSet<String>,
) {
    if requested.is_some_and(|requested| requested != scope) || tags.is_empty() {
        return;
    }
    let (_, out) = by_scope
        .entry(tag_scope_key(scope))
        .or_insert((scope, BTreeSet::new()));
    out.extend(tags);
}
