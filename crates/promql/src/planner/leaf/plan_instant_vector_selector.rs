use super::{
    Arc, Extension, InstantManipulate, InstantSelectorPlan, LabeledSeries, LogicalPlan, Result,
    SampleTimePresence, SeriesLeaf, StepGrid, TIME_COLUMN, Time, TimeExt, VALUE_COLUMN,
    divide_and_normalize, prom_session_context, series_leaf,
};

/// Builds the leaf table and operator chain for a bare instant-vector selector.
///
/// The chain evaluates the selector at every instant of `grid` with the given
/// `lookback_delta`, emitting one row per (series, grid instant) that has a
/// sample in its lookback window. An instant query passes a one-point grid; the
/// range driver passes the query's whole step grid, so one plan covers every
/// step. [`InstantManipulate`] walks the grid with a single monotonic cursor per
/// series, so a step's selection is the same one a one-point grid at that
/// instant would make.
///
/// `series` are the matched series and their float samples over the scan window
/// `(grid.start - lookback_delta, grid.end]`, in ascending fingerprint
/// order with each series' samples in timestamp order — which is the
/// contiguous, time-ordered run [`SeriesDivide`] needs. The caller must filter
/// out the stale-NaN markers before the values reach [`InstantManipulate`]. This
/// matches the staleness handling of the interpreter.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch or the table.
pub async fn plan_instant_vector_selector(
    series: Vec<LabeledSeries>,
    grid: StepGrid,
    lookback_delta: Time,
) -> Result<InstantSelectorPlan> {
    let SeriesLeaf {
        label_names,
        labels_by_fp,
        leaf,
    } = series_leaf(&series, "prom_leaf", SampleTimePresence::Included)?;
    let ctx = prom_session_context();
    let normalize = divide_and_normalize(&label_names, leaf);
    // InstantManipulate selects, for each grid instant, the latest sample
    // within (instant - lookback, instant], dropping NaN.
    let instant = LogicalPlan::Extension(Extension {
        node: Arc::new(InstantManipulate {
            start_ms: grid.start,
            end_ms: grid.end,
            step_ms: grid.step,
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
