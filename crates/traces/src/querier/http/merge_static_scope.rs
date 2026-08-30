use super::{ScopedTag, TagScope, BTreeSet};

pub(crate) fn merge_static_scope(tags: &mut Vec<ScopedTag>, scope: TagScope, static_tags: &[&str]) {
    let existing = tags
        .iter()
        .position(|scoped| scoped.scope == scope)
        .map(|idx| tags.remove(idx).tags)
        .unwrap_or_default();
    let mut seen = BTreeSet::new();
    let mut merged = Vec::new();
    for tag in static_tags {
        if seen.insert((*tag).to_string()) {
            merged.push((*tag).to_string());
        }
    }
    for tag in existing {
        if seen.insert(tag.clone()) {
            merged.push(tag);
        }
    }
    tags.push(ScopedTag {
        scope,
        tags: merged,
    });
}
