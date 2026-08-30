/// Which extremum a [`PromExtremumAccumulator`] tracks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Extremum {
    Min,
    Max,
}

impl Extremum {
    /// Returns true when `candidate` should replace the running value `running`.
    ///
    /// The rule is Prometheus' NaN-ignoring float ordering. A NaN running value
    /// is always replaced. A NaN candidate never replaces a non-NaN running
    /// value.
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
