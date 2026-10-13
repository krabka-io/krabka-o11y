use super::{
    BTreeMap, CompareByAttr, CompareCounts, CompareGroup, CompareTotals, MetricsRange,
    TraceMetricSeries, compare_total_series, compare_value_series,
};

struct RankedCompareValue<'a> {
    value: &'a String,
    selection_total: u64,
}

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
    for count_entry in counts {
        let key = count_entry.0;
        let buckets = count_entry.1;
        by_attr
            .entry(key.attr_key)
            .or_default()
            .entry(key.value)
            .or_default()
            .insert(key.group, buckets);
    }

    let mut series = Vec::new();
    for attr_entry in by_attr {
        let attr_key = attr_entry.0;
        let values = attr_entry.1;
        // Rank values by the SELECTION-group count (descending), tie-broken by
        // value ascending. This yields a single, deterministic top_n value set
        // shared by both groups.
        let mut ranked: Vec<RankedCompareValue<'_>> = values
            .iter()
            .map(|value_entry| {
                let value = value_entry.0;
                let per_group = value_entry.1;
                let selection_total: u64 = per_group
                    .get(&CompareGroup::Selection)
                    .map_or(0, |buckets| buckets.iter().sum());
                RankedCompareValue {
                    value,
                    selection_total,
                }
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.selection_total
                .cmp(&a.selection_total)
                .then_with(|| a.value.cmp(b.value))
        });
        ranked.truncate(top_n);

        // Emit a baseline and a selection series for each chosen value; a value
        // missing from a group is zero-filled so both groups share the value set.
        for ranked_value in ranked {
            let per_group = &values[ranked_value.value];
            for group in [CompareGroup::Baseline, CompareGroup::Selection] {
                let buckets = per_group
                    .get(&group)
                    .cloned()
                    .unwrap_or_else(|| vec![0; bucket_count]);
                series.push(compare_value_series(
                    group,
                    &attr_key,
                    ranked_value.value,
                    &buckets,
                    range,
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
