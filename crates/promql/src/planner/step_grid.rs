/// The evaluation grid an operator chain is driven over.
///
/// Every field is in epoch milliseconds, which is the unit the operators, the
/// leaf batches, and the store rows all speak.
///
/// [`InstantManipulate`] and [`RangeManipulate`] have always taken a grid. The
/// instant planner drives them with a one-point grid; the range driver builds
/// one plan over the query's whole step grid instead of one plan per step.
///
/// [`InstantManipulate`]: crate::extension::instant_manipulate::InstantManipulate
/// [`RangeManipulate`]: crate::extension::range_manipulate::RangeManipulate
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StepGrid {
    /// First evaluation instant, in epoch milliseconds.
    pub start: i64,
    /// Last evaluation instant, in epoch milliseconds. Inclusive.
    pub end: i64,
    /// Stride between evaluation instants, in milliseconds. Always positive.
    pub step: i64,
}

impl StepGrid {
    /// The one-point grid at `time_ms`.
    ///
    /// `stride_ms` is only there to keep the operators' positive-stride
    /// invariant; any positive stride covers exactly one point.
    #[must_use]
    pub fn instant(time_ms: i64, stride_ms: i64) -> Self {
        Self {
            start: time_ms,
            end: time_ms,
            step: stride_ms.max(1),
        }
    }

    /// The number of evaluation instants on the grid.
    #[must_use]
    pub fn point_count(&self) -> usize {
        if self.step <= 0 || self.end < self.start {
            return 0;
        }
        let span = self.end.saturating_sub(self.start);
        usize::try_from(span / self.step).map_or(usize::MAX, |steps| steps.saturating_add(1))
    }

    /// The index of `time_ms` on the grid, or `None` when it is off-grid.
    ///
    /// A time between two grid points, or outside `[start_ms, end_ms]`, is
    /// off-grid. The range driver's leaf cache uses this to tell a request it
    /// can answer from a grid-driven evaluation from one it cannot, such as a
    /// subquery's sub-step.
    #[must_use]
    pub fn index_of(&self, time_ms: i64) -> Option<usize> {
        if self.step <= 0 || time_ms < self.start || time_ms > self.end {
            return None;
        }
        let span = time_ms.checked_sub(self.start)?;
        if span % self.step != 0 {
            return None;
        }
        usize::try_from(span / self.step).ok()
    }
}
