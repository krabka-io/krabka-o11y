use datafusion::{
    execution::FunctionRegistry,
    logical_expr::{Expr, col},
    prelude::SessionContext,
};

/// The rate-family or `*_over_time` UDF a range plan applies to each window.
pub(crate) struct WindowUdf<'a, F> {
    /// The name the UDF is registered under in the session context.
    pub(crate) udf_name: &'a str,
    /// Receives the three windowed columns (the eval timestamp, the timestamp
    /// range and the value range) and the range width in milliseconds, and
    /// returns the UDF's argument list.
    pub(crate) build_args: F,
    /// The result column the call is aliased to.
    pub(crate) value_column: &'a str,
}

use super::{
    Arc, Extension, LogicalPlan, LogicalPlanBuilder, PromqlError, Result, StepGrid, TIME_COLUMN,
    Time, TimeExt, VALUE_COLUMN,
};
use crate::extension::range_manipulate::{RANGE_SUFFIX, RangeManipulate};

/// The per-series input a range plan folds into windows.
pub(crate) struct RangeWindows<'a> {
    /// The divided and timestamp-sorted per-series samples.
    pub(crate) normalize: LogicalPlan,
    pub(crate) grid: StepGrid,
    /// The width of each window `(t - range, t]`.
    pub(crate) range: Time,
    /// The label columns the projection carries through.
    pub(crate) label_names: &'a [String],
}

/// Folds the normalized per-series samples into each grid instant's window
/// `(t - range, t]` and projects the label columns, the `udf` call over that
/// window, and the eval timestamp.
pub(crate) fn range_udf_plan(
    session: &SessionContext,
    windows: RangeWindows<'_>,
    udf: WindowUdf<'_, impl FnOnce([Expr; 3], i64) -> Vec<Expr>>,
) -> Result<LogicalPlan> {
    let RangeWindows {
        normalize,
        grid,
        range,
        label_names,
    } = windows;
    let range_ms = range.millis_i64();
    let range = RangeManipulate::new(
        grid.start,
        grid.end,
        grid.step,
        range_ms,
        TIME_COLUMN.to_string(),
        VALUE_COLUMN.to_string(),
        normalize,
    )
    .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let range = LogicalPlan::Extension(Extension {
        node: Arc::new(range),
    });

    let function = session
        .udf(udf.udf_name)
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let time_range_column = format!("{TIME_COLUMN}{RANGE_SUFFIX}");
    let value_range_column = format!("{VALUE_COLUMN}{RANGE_SUFFIX}");
    let window = [
        col(TIME_COLUMN),
        col(time_range_column),
        col(value_range_column),
    ];
    let call = function
        .call((udf.build_args)(window, range_ms))
        .alias(udf.value_column);

    let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
    projections.push(call);
    // Carry the eval timestamp through, so a grid-driven plan's output says
    // which instant each row belongs to. It is an `Int64` column, so neither the
    // label reader nor the aggregate's grouping-column scan mistakes it for a
    // label.
    projections.push(col(TIME_COLUMN));

    LogicalPlanBuilder::from(range)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))
}
