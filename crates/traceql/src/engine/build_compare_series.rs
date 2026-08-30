use super::{
    BTreeMap, CompareByAttr, CompareCounts, CompareGroup, CompareTotals, MetricsRange,
    TraceMetricSeries, compare_total_series, compare_value_series,
};

/// Builds the compare time series from the accumulated per-bucket counts.
///
/// For each attribute, this function picks ONE shared value set: the `top_n`
/// values with the highest SELECTION-group count. It breaks ties by value in
/// ascending order, which makes the result deterministic. The selection is the
/// group that the user investigates.
///
/// The function emits BOTH a baseline series and a selection series for each
/// of those values, with the label
/// `{__meta_type=<group>, <attribute>=<value>}`. It zero-fills a value that is
/// absent from a group, so the baseline distribution and the selection
/// distribution cover the same values. Grafana's Comparison view shows them
/// side by side. The function emits per-group totals as
/// `{__meta_type=<group>_total}`.
pub(crate) fn build_compare_series(
    counts: CompareCounts,
    totals: &CompareTotals,
    top_n: usize,
    range: MetricsRange,
    bucket_count: usize,
) -> Vec<TraceMetricSeries> {
    // Regroup the flat (group, attr_key, value) entries into attr → value →
    // group → buckets so a single shared value set per attribute can be picked.
    let mut by_attr: CompareByAttr = BTreeMap::new();
    for ((group, attr_key, value), buckets) in counts {
        by_attr
            .entry(attr_key)
            .or_default()
            .entry(value)
            .or_default()
            .insert(group, buckets);
    }

    let mut series = Vec::new();
    for (attr_key, values) in by_attr {
        // Rank values by the SELECTION-group count (descending), tie-broken by
        // value ascending. This yields a single, deterministic top_n value set
        // shared by both groups.
        let mut ranked: Vec<(&String, u64)> = values
            .iter()
            .map(|(value, per_group)| {
                let selection_total: u64 = per_group
                    .get(&CompareGroup::Selection)
                    .map_or(0, |buckets| buckets.iter().sum());
                (value, selection_total)
            })
            .collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        ranked.truncate(top_n);

        // Emit a baseline and a selection series for each chosen value; a value
        // missing from a group is zero-filled so both groups share the value set.
        for (value, _) in ranked {
            let per_group = &values[value];
            for group in [CompareGroup::Baseline, CompareGroup::Selection] {
                let buckets = per_group
                    .get(&group)
                    .cloned()
                    .unwrap_or_else(|| vec![0; bucket_count]);
                series.push(compare_value_series(
                    group, &attr_key, value, &buckets, range,
                ));
            }
        }
    }

    // Per-group totals, including a zero-valued total for any group with no
    // spans so Grafana always has a denominator for both groups.
    for group in [CompareGroup::Baseline, CompareGroup::Selection] {
        let buckets = totals
            .get(&group)
            .cloned()
            .unwrap_or_else(|| vec![0; bucket_count]);
        series.push(compare_total_series(group, &buckets, range));
    }

    series
}
