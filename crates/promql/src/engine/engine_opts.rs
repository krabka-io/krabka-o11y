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
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            lookback_delta: minutes(5),
            eval_interval: minutes(1),
            max_samples: 50_000_000,
        }
    }
}
