
/// Which extremum [`fold_over_time_extremum`] tracks.
#[derive(Clone, Copy)]
pub(crate) enum ExtremumKind {
    Min,
    Max,
}

impl ExtremumKind {
    /// Returns true if `candidate` should replace the running value `running`.
    ///
    /// The rule is Prometheus' NaN-ignoring float order. `AggregateState::push_float`
    /// and the `prom_min`/`prom_max` aggregate UDAF apply the same rule. This
    /// method always replaces a NaN running value. A NaN candidate never
    /// replaces a non-NaN running value, because `NaN > _` and `NaN < _` are
    /// both false.
    pub(crate) fn should_replace(self, running: f64, candidate: f64) -> bool {
        if running.is_nan() {
            return true;
        }
        // Both comparisons are permanent mutation survivors. Loosening either
        // one only lets an *equal* candidate replace the running value, and
        // `running` is a bare `f64`: swapping it for its own equal leaves the
        // fold's result identical. A NaN candidate compares false under all
        // four spellings.
        match self {
            Self::Min => running > candidate,
            Self::Max => running < candidate,
        }
    }
}
