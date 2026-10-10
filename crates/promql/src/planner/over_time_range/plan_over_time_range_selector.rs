use super::{
    Expr, LabeledSeries, OVER_TIME_VALUE_COLUMN, OverTimeFamily, OverTimeFold, OverTimeRangePlan,
    Result, lit, prom_session_context,
};
use crate::planner::{
    RangeWindowGrid,
    leaf::{
        RangeWindows, SampleTimePresence, SeriesLeaf, WindowUdf, divide_and_normalize,
        range_udf_plan, series_leaf,
    },
};

/// Builds the leaf table and operator chain for `f_over_time(selector[range])`.
///
/// The chain evaluates at every instant of `windows.grid` with the
/// `windows.range` width,
/// exactly as [`plan_rate_range_selector`](crate::planner::rate_range::plan_rate_range_selector)
/// does. `fold.phi` is the quantile literal for [`OverTimeFamily::Quantile`], and
/// every other family ignores it.
///
/// `series` are the matched series and their float samples over the exact range
/// window `(grid.start - range, grid.end]`, in ascending fingerprint order
/// with each series' samples in timestamp order — which is the contiguous,
/// time-ordered run [`SeriesDivide`] needs. The caller must filter out stale-NaN
/// markers before the values reach the operator chain. Genuine NaN values pass
/// through unchanged, as the interpreter does.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch, the table,
/// or the projection plan.
pub async fn plan_over_time_range_selector(
    series: Vec<LabeledSeries>,
    windows: RangeWindowGrid,
    fold: OverTimeFold,
) -> Result<OverTimeRangePlan> {
    let RangeWindowGrid { grid, range } = windows;
    let OverTimeFold { family, phi } = fold;
    let SeriesLeaf {
        label_names,
        labels_by_fp,
        leaf,
    } = series_leaf(&series, "prom_over_time_leaf", SampleTimePresence::Omitted)?;
    let ctx = prom_session_context();
    let normalize = divide_and_normalize(&label_names, leaf);
    let plan = range_udf_plan(
        &ctx,
        RangeWindows {
            normalize,
            grid,
            range,
            label_names: &label_names,
        },
        WindowUdf {
            udf_name: family.udf_name(),
            build_args: |window: [Expr; 3], _: i64| {
                // `quantile_over_time` threads the `phi` literal ahead of the
                // windowed columns; the other families take only the three
                // windowed columns.
                let mut args: Vec<Expr> = Vec::with_capacity(4);
                if matches!(family, OverTimeFamily::Quantile) {
                    args.push(lit(phi));
                }
                args.extend(window);
                args
            },
            value_column: OVER_TIME_VALUE_COLUMN,
        },
    )?;

    Ok(OverTimeRangePlan {
        ctx,
        plan,
        labels_by_fp,
    })
}
