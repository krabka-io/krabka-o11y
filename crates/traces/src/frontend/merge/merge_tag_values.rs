use super::{BTreeSet, Metrics, TagValuesPartial, TypedValue};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergedTagValues {
    pub values: Vec<TypedValue>,
    pub metrics: Metrics,
}

/// Union typed tag values across jobs, then dedup the `(type, value)` pairs.
/// This also accumulates metrics.
#[must_use]
pub fn merge_tag_values(partials: Vec<TagValuesPartial>) -> MergedTagValues {
    let mut metrics = Metrics::default();
    let mut seen: BTreeSet<TypedValue> = BTreeSet::new();
    let mut out = Vec::new();

    for partial in partials {
        metrics.add(&partial.metrics);
        for v in partial.values {
            if seen.insert(v.clone()) {
                out.push(v);
            }
        }
    }
    out.sort_by(|left, right| {
        left.type_
            .cmp(&right.type_)
            .then_with(|| left.value.cmp(&right.value))
    });
    MergedTagValues {
        values: out,
        metrics,
    }
}
