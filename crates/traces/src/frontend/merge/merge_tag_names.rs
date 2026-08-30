use super::*;

/// Union scoped tag names across jobs, then dedup and sort per scope. This also
/// accumulates metrics.
#[must_use]
pub fn merge_tag_names(partials: Vec<TagNamesPartial>) -> (Vec<ScopedTag>, Metrics) {
    let mut metrics = Metrics::default();
    // Keyed on a stable scope discriminant so the merged scopes have a
    // deterministic order without requiring `Ord` on `TagScope`.
    let mut by_scope: std::collections::BTreeMap<&'static str, (TagScope, BTreeSet<String>)> =
        std::collections::BTreeMap::new();

    for partial in partials {
        metrics.add(&partial.metrics);
        for st in partial.tags {
            let key = scope_key(st.scope);
            let entry = by_scope
                .entry(key)
                .or_insert_with(|| (st.scope, BTreeSet::new()));
            entry.1.extend(st.tags);
        }
    }

    let merged = by_scope
        .into_values()
        .map(|(scope, set)| ScopedTag {
            scope,
            tags: set.into_iter().collect(),
        })
        .collect();
    (merged, metrics)
}
