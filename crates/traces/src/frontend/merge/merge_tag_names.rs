use super::{BTreeSet, Metrics, ScopedTag, TagNamesPartial, TagScope, scope_key};

#[derive(Clone, Debug, PartialEq)]
pub struct MergedTagNames {
    pub tags: Vec<ScopedTag>,
    pub metrics: Metrics,
}

/// Union scoped tag names across jobs, then dedup and sort per scope. This also
/// accumulates metrics.
#[must_use]
pub fn merge_tag_names(partials: Vec<TagNamesPartial>) -> MergedTagNames {
    let mut metrics = Metrics::default();
    // Keyed on a stable scope discriminant so the merged scopes have a
    // deterministic order without requiring `Ord` on `TagScope`.
    let mut by_scope: std::collections::BTreeMap<&'static str, ScopedTagAccumulator> =
        std::collections::BTreeMap::new();

    for partial in partials {
        metrics.add(&partial.metrics);
        for st in partial.tags {
            let key = scope_key(st.scope);
            let entry = by_scope.entry(key).or_insert_with(|| ScopedTagAccumulator {
                scope: st.scope,
                tags: BTreeSet::new(),
            });
            entry.tags.extend(st.tags);
        }
    }

    let merged = by_scope
        .into_values()
        .map(|accumulator| ScopedTag {
            scope: accumulator.scope,
            tags: accumulator.tags.into_iter().collect(),
        })
        .collect();
    MergedTagNames {
        tags: merged,
        metrics,
    }
}

struct ScopedTagAccumulator {
    scope: TagScope,
    tags: BTreeSet<String>,
}
