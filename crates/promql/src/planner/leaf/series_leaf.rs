use super::{
    Arc, BTreeSet, Extension, LabeledSeries, Labels, LogicalPlan, Result, SampleTimePresence,
    SeriesDivide, SeriesFingerprint, SeriesNormalize, TIME_COLUMN, build_leaf_batch, leaf_scan,
    leaf_schema,
};

/// The leaf scan over a selector's matched series, with what the plan needs
/// to read its output back.
pub(crate) struct SeriesLeaf {
    /// The distinct label names across the series, in order: the label columns.
    pub(crate) label_names: Vec<String>,
    pub(crate) labels_by_fp: std::collections::BTreeMap<SeriesFingerprint, Labels>,
    pub(crate) leaf: LogicalPlan,
}

/// Builds the leaf table over `series`, named `table_name` in `EXPLAIN` output.
///
/// The label names across all matched series become the label columns carried
/// through the operator chain. `sample_time` says whether it carries the sample-time column.
pub(crate) fn series_leaf(
    series: &[LabeledSeries],
    table_name: &str,
    sample_time: SampleTimePresence,
) -> Result<SeriesLeaf> {
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut labels_by_fp = std::collections::BTreeMap::new();
    for one in series {
        for (name, _) in one.labels.iter() {
            label_names.insert(name.clone());
        }
        labels_by_fp
            .entry(one.fp)
            .or_insert_with(|| (*one.labels).clone());
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    let schema = leaf_schema(&label_names, sample_time);
    let batch = build_leaf_batch(Arc::clone(&schema), &label_names, series)?;
    let leaf = leaf_scan(table_name, schema, batch)?;
    Ok(SeriesLeaf {
        label_names,
        labels_by_fp,
        leaf,
    })
}

/// Splits the sorted leaf into per-series batches and sorts each by timestamp.
///
/// [`SeriesDivide`] on every label column splits the input into exact
/// per-series batches. [`SeriesNormalize`] then sorts each batch by timestamp.
/// The offset is already folded into the grid by the caller, so it is zero
/// here. NaN is NOT filtered: selectors keep genuine NaN (only stale-NaN is
/// dropped, which the caller already did), so the operator chain must not
/// strip it.
pub(crate) fn divide_and_normalize(label_names: &[String], leaf: LogicalPlan) -> LogicalPlan {
    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide {
            tag_columns: label_names.to_vec(),
            input: leaf,
        }),
    });
    LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesNormalize {
            offset_ms: 0,
            time_index: TIME_COLUMN.to_string(),
            need_filter_out_nan: false,
            input: divide,
        }),
    })
}
