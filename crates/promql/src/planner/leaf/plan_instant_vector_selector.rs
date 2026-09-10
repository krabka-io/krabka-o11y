use super::{
    Arc, BTreeSet, Extension, InstantManipulate, InstantSelectorPlan, LabeledSeries, LogicalPlan,
    MemTable, PromqlError, Result, SeriesDivide, SeriesNormalize, TIME_COLUMN, Time, TimeExt,
    VALUE_COLUMN, build_leaf_batch, leaf_schema, prom_session_context,
};

/// Builds the leaf table and operator chain for a bare instant-vector selector.
///
/// The chain evaluates the selector at `eval_time_ms` with the given
/// `lookback_delta`. `series` are the matched series and their float samples
/// over the scan window `(eval_time_ms - lookback_delta, eval_time_ms]`, in
/// ascending fingerprint order with each series' samples in timestamp order —
/// which is the contiguous, time-ordered run [`SeriesDivide`] needs. The caller
/// must filter out the stale-NaN markers before the values reach
/// [`InstantManipulate`]. This matches the staleness handling of the
/// interpreter.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch or the table.
pub async fn plan_instant_vector_selector(
    series: Vec<LabeledSeries>,
    eval_time_ms: i64,
    lookback_delta: Time,
) -> Result<InstantSelectorPlan> {
    // Collect the distinct label names across all matched series; these become
    // the label columns carried through the operator chain.
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut labels_by_fp = std::collections::BTreeMap::new();
    for one in &series {
        for (name, _) in one.labels.iter() {
            label_names.insert(name.clone());
        }
        labels_by_fp
            .entry(one.fp)
            .or_insert_with(|| (*one.labels).clone());
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    let schema = leaf_schema(&label_names);
    let batch = build_leaf_batch(Arc::clone(&schema), &label_names, &series)?;

    let ctx = prom_session_context();
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    ctx.register_table("prom_leaf", Arc::new(table))?;
    let leaf = ctx.table("prom_leaf").await?.into_optimized_plan()?;

    // SeriesDivide on every label column splits the sorted input into exact
    // per-series batches.
    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide {
            tag_columns: label_names.clone(),
            input: leaf,
        }),
    });
    // SeriesNormalize sorts each per-series batch by timestamp. The offset is
    // already folded into eval_time_ms by the caller, so it is zero here.
    let normalize = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesNormalize {
            offset_ms: 0,
            time_index: TIME_COLUMN.to_string(),
            need_filter_out_nan: false,
            input: divide,
        }),
    });
    // InstantManipulate selects, for the single eval step, the latest sample
    // within (eval_time - lookback, eval_time], dropping NaN.
    let instant = LogicalPlan::Extension(Extension {
        node: Arc::new(InstantManipulate {
            start_ms: eval_time_ms,
            end_ms: eval_time_ms,
            // A single grid step: any positive stride covers exactly one point.
            step_ms: lookback_delta.millis_i64().max(1),
            lookback_delta_ms: lookback_delta.millis_i64(),
            time_index: TIME_COLUMN.to_string(),
            field_column: VALUE_COLUMN.to_string(),
            input: normalize,
        }),
    });

    Ok(InstantSelectorPlan {
        ctx,
        plan: instant,
        labels_by_fp,
    })
}
