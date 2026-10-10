use super::{InstantKind, RangeKind, RateWindow, extrapolated_rate, instant_delta};

/// Which rate-family function a [`RateUdf`] evaluates.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum RateFamily {
    /// Windowed, reset-corrected, per-second rate.
    Rate,
    /// Windowed, reset-corrected total increase.
    Increase,
    /// Windowed gauge delta (first..last, no reset correction).
    Delta,
    /// Instant per-second rate from the last two samples.
    Irate,
    /// Instant gauge delta from the last two samples.
    Idelta,
}

impl RateFamily {
    pub(crate) fn udf_name(self) -> &'static str {
        match self {
            Self::Rate => "prom_rate",
            Self::Increase => "prom_increase",
            Self::Delta => "prom_delta",
            Self::Irate => "prom_irate",
            Self::Idelta => "prom_idelta",
        }
    }

    /// Evaluates one window and returns `None` where Prometheus has no value.
    ///
    /// `window.bounds.range_end_ms` is the eval timestamp, and `window.range`
    /// is the selector width.
    pub(crate) fn eval_window(self, window: RateWindow<'_>) -> Option<f64> {
        match self {
            Self::Rate => extrapolated_rate(window, RangeKind::Rate),
            Self::Increase => extrapolated_rate(window, RangeKind::Increase),
            Self::Delta => extrapolated_rate(window, RangeKind::Delta),
            Self::Irate => instant_delta(window.timestamps, window.values, InstantKind::Irate),
            Self::Idelta => instant_delta(window.timestamps, window.values, InstantKind::Idelta),
        }
    }
}
