/// The observation-value bounds of `histogram_fraction(lower, upper, v)`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FractionBounds {
    /// The lower bound, exclusive.
    pub(crate) lower: f64,
    /// The upper bound, inclusive.
    pub(crate) upper: f64,
}
