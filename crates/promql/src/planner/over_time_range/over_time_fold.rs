use crate::functions::OverTimeFamily;

/// Which `*_over_time` function a fold applies, with its quantile literal.
#[derive(Clone, Copy, Debug)]
pub struct OverTimeFold {
    /// The `*_over_time` function.
    pub family: OverTimeFamily,
    /// The quantile literal for [`OverTimeFamily::Quantile`]. Every other
    /// family ignores it.
    pub phi: f64,
}
