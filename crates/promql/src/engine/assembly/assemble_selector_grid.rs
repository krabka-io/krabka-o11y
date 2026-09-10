use super::{
    Array, BTreeMap, Float64Array, GridPoint, Int64Array, PromqlError, RecordBatch, Result,
    SeriesFingerprint, StepGrid, labels_from_batch, leaf, row_fingerprints,
};

/// Assembles a grid-driven instant-vector-selector plan's output into one point
/// list per grid instant.
///
/// This is [`assemble_selector_batches`](super::assemble_selector_batches) over
/// a whole grid rather than one instant. Output rows carry label columns plus
/// `timestamp` — the grid instant [`InstantManipulate`] selected for — and
/// `sample_timestamp`, the selected sample's own timestamp, which is what the
/// assembled sample reports. A row whose `timestamp` is off the grid cannot
/// arise, since the operator emits only grid instants, and is dropped.
///
/// Points come out in fingerprint order within each instant, matching what the
/// per-instant assembler's `BTreeMap` emits.
///
/// [`InstantManipulate`]: crate::extension::instant_manipulate::InstantManipulate
///
/// # Errors
///
/// Returns [`PromqlError::Exec`] if a batch is missing one of the columns the
/// selector shape defines.
pub(crate) fn assemble_selector_grid(
    batches: &[RecordBatch],
    grid: StepGrid,
) -> Result<Vec<Vec<GridPoint>>> {
    let mut steps: Vec<BTreeMap<SeriesFingerprint, (i64, f64)>> =
        vec![BTreeMap::new(); grid.point_count()];
    for batch in batches {
        let grid_timestamps = int64_column(batch, leaf::TIME_COLUMN)?;
        let sample_timestamps = int64_column(batch, leaf::SAMPLE_TIME_COLUMN)?;
        let values = batch
            .column_by_name(leaf::VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("planner leaf missing Float64 value column".to_string())
            })?;
        for (row, fp) in row_fingerprints(batch, labels_from_batch)
            .into_iter()
            .enumerate()
        {
            let Some(index) = grid.index_of(grid_timestamps.value(row)) else {
                continue;
            };
            let Some(step) = steps.get_mut(index) else {
                continue;
            };
            let sample_ts_ms = sample_timestamps.value(row);
            let value = values.value(row);
            step.entry(fp)
                .and_modify(|latest| {
                    if sample_ts_ms > latest.0 {
                        *latest = (sample_ts_ms, value);
                    }
                })
                .or_insert((sample_ts_ms, value));
        }
    }
    Ok(steps
        .into_iter()
        .map(|step| {
            step.into_iter()
                .map(|(fp, (ts_ms, value))| GridPoint { fp, ts_ms, value })
                .collect()
        })
        .collect())
}

fn int64_column<'batch>(batch: &'batch RecordBatch, name: &str) -> Result<&'batch Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| PromqlError::Exec(format!("planner leaf missing Int64 `{name}` column")))
}
