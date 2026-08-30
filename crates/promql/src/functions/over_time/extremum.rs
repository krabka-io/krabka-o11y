use super::*;

/// Which extremum [`fold_extremum`] tracks.
#[derive(Clone, Copy)]
pub(crate) enum Extremum {
    Min,
    Max,
}

impl Extremum {
    /// Returns true if `candidate` should replace the running value `running`.
    ///
    /// The rule is Prometheus' NaN-ignoring float order. The `prom_min` and
    /// `prom_max` aggregate UDAF's `Extremum::should_replace` and the engine's
    /// `AggregateState::push_float` use the same rule. This method always
    /// replaces a NaN running value. A NaN candidate never replaces a non-NaN
    /// running value, because `NaN > _` and `NaN < _` are both false.
    pub(crate) fn should_replace(self, running: f64, candidate: f64) -> bool {
        if running.is_nan() {
            return true;
        }
        match self {
            Self::Min => running > candidate,
            Self::Max => running < candidate,
        }
    }
}
