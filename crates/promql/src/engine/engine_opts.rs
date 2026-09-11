use super::{Time, minutes};

/// Static options for `PromQL` evaluation.
///
/// This type is not `Eq`, because the two windows are [`Time`] quantities that
/// store `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EngineOpts {
    /// Maximum age of a sample considered by an instant-vector selector.
    pub lookback_delta: Time,
    /// Global evaluation interval used when a subquery omits its resolution.
    pub eval_interval: Time,
    /// Maximum float samples returned by one query.
    pub max_samples: usize,
    /// Maximum series one query may select. `0` turns the cap off, which is the
    /// same sentinel as `Limits::max_fetched_series_per_query`.
    pub max_fetched_series: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            lookback_delta: minutes(5),
            eval_interval: minutes(1),
            max_samples: 50_000_000,
            max_fetched_series: 0,
        }
    }
}
