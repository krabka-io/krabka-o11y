use super::{Time, TimeExt, extrapolated_rate, RangeKind, instant_delta, InstantKind};

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
    /// `eval_ts` is `range_end_ms`. `range` is the selector width.
    pub(crate) fn eval_window(
        self,
        timestamps: &[i64],
        values: &[f64],
        eval_ts: i64,
        range: Time,
    ) -> Option<f64> {
        let range_ms = range.millis_i64();
        match self {
            Self::Rate => extrapolated_rate(
                timestamps,
                values,
                eval_ts - range_ms,
                eval_ts,
                range,
                RangeKind::Rate,
            ),
            Self::Increase => extrapolated_rate(
                timestamps,
                values,
                eval_ts - range_ms,
                eval_ts,
                range,
                RangeKind::Increase,
            ),
            Self::Delta => extrapolated_rate(
                timestamps,
                values,
                eval_ts - range_ms,
                eval_ts,
                range,
                RangeKind::Delta,
            ),
            Self::Irate => instant_delta(timestamps, values, InstantKind::Irate),
            Self::Idelta => instant_delta(timestamps, values, InstantKind::Idelta),
        }
    }
}
