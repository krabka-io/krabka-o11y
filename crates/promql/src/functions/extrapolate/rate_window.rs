use super::{Time, WindowBounds};

/// One series' float samples in a rate-family window, and the window itself.
#[derive(Clone, Copy)]
pub(crate) struct RateWindow<'a> {
    /// Sample timestamps in epoch milliseconds, ascending.
    pub(crate) timestamps: &'a [i64],
    /// Sample values, one per timestamp.
    pub(crate) values: &'a [f64],
    /// The window's bounds.
    pub(crate) bounds: WindowBounds,
    /// The selector range the rate is taken over.
    pub(crate) range: Time,
}
