use super::{
    Expr, LabeledSeries, LogicalPlanBuilder, RATE_VALUE_COLUMN, RateRangePlan, RateUdfKind, Result,
    TIME_COLUMN, VALUE_COLUMN, col, lit, prom_session_context,
};
use crate::planner::{
    RangeWindowGrid,
    leaf::{
        RangeWindows, SampleTimePresence, SeriesLeaf, WindowUdf, divide_and_normalize,
        range_udf_plan, series_leaf,
    },
};

/// Builds the leaf table and operator chain that evaluates `f(selector[range])`
/// at every instant of `windows.grid` with the `windows.range` width.
///
/// An instant query passes a one-point grid; the range driver passes the query's
/// whole step grid, so one plan covers every step. [`RangeManipulate`] derives
/// each instant's window `(instant - range, instant]` with a single pair of
/// monotonic edges, so a step's window is the one a one-point grid at that
/// instant would produce. The projection carries the eval timestamp through, so
/// each output row says which grid instant it belongs to.
///
/// `series` are the matched series and their float samples over the exact range
/// window `(grid.start - range, grid.end]`, in ascending fingerprint order
/// with each series' samples in timestamp order — which is the contiguous,
/// time-ordered run [`SeriesDivide`] needs. The caller must filter out stale-NaN
/// markers before the values reach the operator chain, which matches the
/// interpreter's `eval_matrix_selector` staleness handling. Genuine NaN values
/// pass through unchanged, as the interpreter does.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch, the table, or
/// the projection plan.
pub async fn plan_rate_range_selector(
    mut series: Vec<LabeledSeries>,
    windows: RangeWindowGrid,
    kind: RateUdfKind,
) -> Result<RateRangePlan> {
    let RangeWindowGrid { grid, range } = windows;
    let created = series.iter().any(|series| {
        series
            .samples
            .iter()
            .any(|sample| sample.start_timestamp_ms.is_some())
    });
    if created {
        super::fold_start_timestamp_rates::fold_start_timestamp_rates(&mut series, windows, kind);
    }

    let SeriesLeaf {
        label_names,
        labels_by_fp,
        leaf,
    } = series_leaf(&series, "prom_rate_leaf", SampleTimePresence::Omitted)?;
    let ctx = prom_session_context();

    if created {
        let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
        projections.push(col(VALUE_COLUMN).alias(RATE_VALUE_COLUMN));
        projections.push(col(TIME_COLUMN));
        let plan = LogicalPlanBuilder::from(leaf)
            .project(projections)?
            .build()?;
        return Ok(RateRangePlan {
            ctx,
            plan,
            labels_by_fp,
        });
    }

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
            udf_name: kind.udf_name(),
            build_args: |window: [Expr; 3], range_ms: i64| {
                let mut args = window.to_vec();
                args.push(lit(range_ms));
                args
            },
            value_column: RATE_VALUE_COLUMN,
        },
    )?;

    Ok(RateRangePlan {
        ctx,
        plan,
        labels_by_fp,
    })
}
