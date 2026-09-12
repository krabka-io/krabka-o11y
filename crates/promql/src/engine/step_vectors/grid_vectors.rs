use super::{BTreeMap, GridPoint, InstantSample, Labels, SampleValue, SeriesFingerprint, StepGrid};

/// One leaf's result at every instant of a range query's step grid.
pub(crate) struct GridVectors {
    /// The grid the results are indexed by. This is the range query's own grid,
    /// not the leaf's offset-shifted evaluation grid, so a lookup takes the step
    /// instant the driver is at.
    grid: StepGrid,
    /// The result label set of each series, already carrying whatever the leaf's
    /// shape does to it. Pending `__name__` removal is tracked separately so
    /// outer label-aware expressions can still observe the metric name.
    labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
    drop_name: bool,
    /// The points of each grid instant, indexed by the instant's grid position
    /// and ordered by fingerprint within it.
    ///
    /// Fingerprint order is the order the per-step assemblers emit, and a float
    /// fold over the vector — `sum`, `avg` — depends on it, so it is part of the
    /// contract rather than an incidental detail.
    steps: Vec<Vec<GridPoint>>,
}

impl GridVectors {
    /// Builds the memo from a grid-driven leaf's assembled per-step points.
    pub(crate) fn new(
        grid: StepGrid,
        labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
        steps: Vec<Vec<GridPoint>>,
        drop_name: bool,
    ) -> Self {
        Self {
            grid,
            labels_by_fp,
            drop_name,
            steps,
        }
    }

    /// The total number of points held, for the size budget.
    pub(crate) fn point_count(&self) -> usize {
        self.steps.iter().map(Vec::len).sum()
    }

    /// The leaf's instant vector at `time_ms`, or `None` when `time_ms` is not a
    /// grid instant.
    ///
    /// A point whose fingerprint has no label set is dropped, which is what the
    /// per-step assemblers do with the same lookup.
    pub(crate) fn vector_at(&self, time_ms: i64) -> Option<Vec<InstantSample>> {
        let index = self.grid.index_of(time_ms)?;
        let points = self.steps.get(index)?;
        Some(
            points
                .iter()
                .filter_map(|point| {
                    self.labels_by_fp
                        .get(&point.fp)
                        .map(|labels| InstantSample {
                            labels: labels.clone(),
                            ts_ms: point.ts_ms,
                            value: SampleValue::Float(point.value),
                            drop_name: self.drop_name,
                        })
                })
                .collect(),
        )
    }
}
