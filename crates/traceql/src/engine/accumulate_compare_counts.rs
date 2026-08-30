use super::{
    AsArray, BTreeMap, COL_SPAN_ID, COL_START, COL_TRACE_ID, CompareCounts, CompareGroup,
    CompareRegexCache, CompareSpec, CompareTotals, HashMap, HashSet, MetricsRange, RecordBatch,
    Result, TraceqlError, UnixNano, collect_selection_regexes, compare_group_for_row, compare_row,
    fixed_8, fixed_16,
};

/// Scans the batches and accumulates per-bucket span counts.
///
/// The counts are `counts[(group, attr_key, value)][bucket]` and
/// `totals[group][bucket]`. `COMPARE_MAX_VALUES_PER_ATTR` bounds the number of
/// DISTINCT values tracked per `(group, attr_key)`, clamped up to `top_n`.
/// When the count reaches that bound, this function drops new distinct values,
/// and the values it already tracks keep counting. Memory stays at
/// `O(attrs * cap * buckets)` for any attribute cardinality.
pub(crate) fn accumulate_compare_counts(
    batches: &[RecordBatch],
    compare: &CompareSpec,
    range: MetricsRange,
    bucket_count: usize,
    max_values_per_attr: usize,
    selected_spans: Option<&HashSet<([u8; 16], [u8; 8])>>,
) -> Result<(CompareCounts, CompareTotals)> {
    let mut counts: CompareCounts = BTreeMap::new();
    let mut totals: CompareTotals = BTreeMap::new();
    // Distinct values already tracked per (group, attr_key), to enforce
    // the configured per-attribute cap during accumulation.
    let mut distinct_per_attr: BTreeMap<(CompareGroup, String), usize> = BTreeMap::new();
    // The cap must never fall below top_n or the final cut would be starved.
    let value_cap = max_values_per_attr.max(compare.top_n);
    // Compile every selection regex once, up front, and reuse across all rows.
    let mut regexes: CompareRegexCache = HashMap::new();
    collect_selection_regexes(&compare.selection, &mut regexes);

    for batch in batches {
        let starts = batch
            .column_by_name(COL_START)
            .ok_or_else(|| TraceqlError::Exec(format!("missing column {COL_START}")))?
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            let ts = UnixNano(starts.value(row));
            if ts < range.scan_start || ts > range.scan_end {
                continue;
            }
            let bucket = usize::try_from((ts.0 - range.scan_start.0) / range.step.0)
                .map_err(|e| TraceqlError::Exec(e.to_string()))?;
            let compare_row = compare_row(batch, row, ts)?;
            let selected_by_plan = if let Some(selected) = selected_spans {
                Some(selected.contains(&(
                    fixed_16(batch, COL_TRACE_ID, row)?,
                    fixed_8(batch, COL_SPAN_ID, row)?,
                )))
            } else {
                None
            };
            let group = compare_group_for_row(&compare_row, compare, &regexes, selected_by_plan);
            let group_totals = totals.entry(group).or_insert_with(|| vec![0; bucket_count]);
            if let Some(slot) = group_totals.get_mut(bucket) {
                *slot += 1;
            }
            for (attr_key, value) in &compare_row.attrs {
                let key = (group, attr_key.clone(), value.clone());
                // An already-tracked value keeps counting regardless of cap; a
                // new distinct value is only tracked while under the per-attr cap.
                if !counts.contains_key(&key) {
                    let distinct = distinct_per_attr
                        .entry((group, attr_key.clone()))
                        .or_insert(0);
                    if *distinct >= value_cap {
                        continue;
                    }
                    *distinct += 1;
                }
                let series = counts.entry(key).or_insert_with(|| vec![0; bucket_count]);
                if let Some(slot) = series.get_mut(bucket) {
                    *slot += 1;
                }
            }
        }
    }
    Ok((counts, totals))
}
