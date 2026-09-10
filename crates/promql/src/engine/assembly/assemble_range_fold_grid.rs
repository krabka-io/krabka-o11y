use super::{
    Array, BTreeMap, Float64Array, GridPoint, Int64Array, PromqlError, RecordBatch, Result,
    SeriesFingerprint, StepGrid, labels_from_rate_batch, leaf, row_fingerprints,
};

/// Assembles a grid-driven rate-family or `*_over_time` projection's output into
/// one point list per grid instant.
///
/// This is [`assemble_rate_batches`](super::assemble_rate_batches) and
/// [`assemble_over_time_batches`](super::assemble_over_time_batches) over a
/// whole grid rather than one instant; the two differ only in the result label
/// set, which the caller applies. Output rows carry label columns, the float
/// `value` the fold produced, and the eval `timestamp` the projection carries
/// through. A NULL value is the UDF's no-value marker — too few samples, or an
/// empty window — and is dropped, exactly as the per-instant assemblers drop it.
/// A non-null NaN is a genuine NaN value and is kept.
///
/// `plan_grid` is the grid the plan ran on, which the selector's `offset` has
/// already been folded into, and is what the batches' eval timestamps are on.
/// `report_grid` is the range query's own grid, whose instant is the timestamp
/// the assembled sample reports — matching the per-instant assemblers, which are
/// handed the driver's step time rather than the offset-shifted one. The two
/// grids share a stride and a point count, so a position on one is the same
/// position on the other.
///
/// `value_column` names the fold's result column, which the rate and
/// `*_over_time` projections alias differently.
///
/// # Errors
///
/// Returns [`PromqlError::Exec`] if a batch is missing the value or eval
/// timestamp column.
pub(crate) fn assemble_range_fold_grid(
    batches: &[RecordBatch],
    plan_grid: StepGrid,
    report_grid: StepGrid,
    value_column: &str,
) -> Result<Vec<Vec<GridPoint>>> {
    let mut steps: Vec<BTreeMap<SeriesFingerprint, f64>> =
        vec![BTreeMap::new(); plan_grid.point_count()];
    for batch in batches {
        let values = batch
            .column_by_name(value_column)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec(format!(
                    "range-fold projection missing Float64 `{value_column}` column"
                ))
            })?;
        let grid_timestamps = batch
            .column_by_name(leaf::TIME_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                PromqlError::Exec(
                    "range-fold projection missing Int64 eval timestamp column".to_string(),
                )
            })?;
        for (row, fp) in row_fingerprints(batch, labels_from_rate_batch)
            .into_iter()
            .enumerate()
        {
            if values.is_null(row) {
                continue;
            }
            let Some(index) = plan_grid.index_of(grid_timestamps.value(row)) else {
                continue;
            };
            let Some(step) = steps.get_mut(index) else {
                continue;
            };
            step.insert(fp, values.value(row));
        }
    }
    let mut out: Vec<Vec<GridPoint>> = Vec::with_capacity(steps.len());
    for (index, step) in steps.into_iter().enumerate() {
        let offset = i64::try_from(index)
            .ok()
            .and_then(|index| index.checked_mul(report_grid.step))
            .ok_or_else(|| PromqlError::Exec("range-fold grid instant overflow".to_string()))?;
        let ts_ms = report_grid.start.saturating_add(offset);
        out.push(
            step.into_iter()
                .map(|(fp, value)| GridPoint { fp, ts_ms, value })
                .collect(),
        );
    }
    Ok(out)
}
