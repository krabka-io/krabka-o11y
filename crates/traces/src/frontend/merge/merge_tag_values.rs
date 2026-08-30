use super::{BTreeSet, Metrics, TagValuesPartial, TypedValue};

/// Union typed tag values across jobs, then dedup the `(type, value)` pairs.
/// This also accumulates metrics.
#[must_use]
pub fn merge_tag_values(partials: Vec<TagValuesPartial>) -> (Vec<TypedValue>, Metrics) {
    let mut metrics = Metrics::default();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut out = Vec::new();

    for partial in partials {
        metrics.add(&partial.metrics);
        for v in partial.values {
            if seen.insert((v.type_.clone(), v.value.clone())) {
                out.push(v);
            }
        }
    }
    out.sort_by(|a, b| (&a.type_, &a.value).cmp(&(&b.type_, &b.value)));
    (out, metrics)
}
