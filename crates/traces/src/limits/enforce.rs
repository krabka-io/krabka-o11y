use std::sync::Arc;

use dashmap::DashMap;
use krabka_units::{
    Time,
    convert::{ByteSizeExt as _, FrequencyExt as _, TimeExt as _},
};
use num_traits::ToPrimitive as _;
use rate_bucket::RateBucket;

use super::{LimitError, Limits};

mod rate_bucket {
    use std::{sync::Mutex, time::Instant};

    /// Token bucket with a separately-configured refill rate and burst
    /// capacity, and with all-or-nothing consumption.
    ///
    /// The broker's `TokenBucket` couples capacity to the refill rate, because
    /// a single `set_rate` sets both, and it consumes only partially. Neither
    /// behaviour fits the traces ingest limiter, which needs a distinct burst
    /// capacity and reject-without-spend semantics. The refill arithmetic
    /// mirrors the broker bucket: tokens accrue at `rate_per_sec` and saturate
    /// at `capacity`.
    #[derive(Debug)]
    pub struct RateBucket {
        rate_per_sec: u64,
        capacity: u64,
        state: Mutex<State>,
    }

    #[derive(Debug)]
    struct State {
        available: u64,
        last_refill: Instant,
    }

    impl RateBucket {
        pub fn new(rate_per_sec: u64, capacity: u64) -> Self {
            Self {
                rate_per_sec,
                capacity,
                state: Mutex::new(State {
                    available: capacity,
                    last_refill: Instant::now(),
                }),
            }
        }

        /// Consume `requested` tokens all-or-nothing.
        ///
        /// This returns `true` and spends the tokens if the full amount is
        /// available after refill. Otherwise it returns `false` and spends
        /// nothing.
        pub fn try_consume_all(&self, requested: u64) -> bool {
            let now = Instant::now();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let elapsed = now.saturating_duration_since(state.last_refill);
            // Tokens accrued = elapsed_nanos * rate / 1e9, saturated into u64.
            let accrued = elapsed.as_nanos() * u128::from(self.rate_per_sec) / 1_000_000_000;
            let refill = u64::try_from(accrued).unwrap_or(u64::MAX);
            state.available = state.available.saturating_add(refill).min(self.capacity);
            state.last_refill = now;
            if state.available >= requested {
                state.available -= requested;
                true
            } else {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::{bytes, hours, per_sec};

    use super::*;
    use crate::limits::{LimitError, Limits};

    /// `f64_from_u64` converts through a decimal string rather than casting,
    /// so a limit above 2^53 keeps its magnitude instead of being rounded by
    /// the cast. The answers avoid 0, 1 and -1, which are exactly the
    /// constants a collapsed body returns.
    #[test]
    fn a_u64_limit_converts_to_a_float_of_the_same_magnitude() {
        let convert = super::f64_from_u64;
        let close = |left: f64, right: f64| (left - right).abs() < f64::EPSILON;

        check!(close(convert(0), 0.0));
        check!(close(convert(7), 7.0));
        check!(close(convert(1_000_000), 1_000_000.0));

        // u64::MAX is about 1.8e19. A float cannot hold it exactly, but it
        // must not become infinity, one, or a rounded-to-nothing zero.
        check!(convert(u64::MAX) > 1.8e19);
        check!(convert(u64::MAX) < 1.9e19);
        check!(convert(u64::MAX).is_finite());
    }

    fn limits_with(spans: u64, attr_bytes: u32) -> Limits {
        Limits {
            max_spans_per_trace: spans,
            max_attribute: bytes(attr_bytes),
            ..Limits::default()
        }
    }

    #[test]
    fn trace_size_cap_rejects_over_limit() {
        let limits = limits_with(100, 2048);

        assert2::assert!(IngestEnforcer::check_trace_size(&limits, 100).is_ok());
        assert2::assert!(matches!(
            IngestEnforcer::check_trace_size(&limits, 101),
            Err(LimitError::MaxSpansPerTrace { .. })
        ));
    }

    #[test]
    fn zero_trace_size_cap_is_unlimited() {
        let limits = limits_with(0, 2048);

        assert2::assert!(IngestEnforcer::check_trace_size(&limits, 5_000_000).is_ok());
    }

    #[test]
    fn attribute_size_cap_enforced() {
        let limits = limits_with(0, 4);

        for (attrs, want) in [
            (vec![("ab".to_string(), 2_u64)], Ok(())),
            (
                // Over-long key.
                vec![("toolong".to_string(), 1_u64)],
                Err(LimitError::AttributeTooLong {
                    limit: 4,
                    observed: 7,
                }),
            ),
            (
                // Over-long value.
                vec![("a".to_string(), 7_u64)],
                Err(LimitError::AttributeTooLong {
                    limit: 4,
                    observed: 7,
                }),
            ),
        ] {
            assert2::assert!(IngestEnforcer::check_attributes(&limits, &attrs) == want);
        }
    }

    #[test]
    fn attribute_size_cap_measures_true_value_bytes() {
        // A value whose TRUE byte length exceeds the cap must be rejected even
        // when a stringification would under-report it (e.g. `Bytes`).
        let limits = limits_with(0, 4);
        let oversized_bytes = vec![("k".to_string(), 8_u64)];
        assert2::assert!(matches!(
            IngestEnforcer::check_attributes(&limits, &oversized_bytes),
            Err(LimitError::AttributeTooLong { .. })
        ));
    }

    #[test]
    fn ingest_rate_bucket_eventually_rejects() {
        let enforcer = IngestEnforcer::new();
        let limits = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_spans: 100,
            ..Limits::default()
        };

        assert2::assert!(enforcer.check_span_rate(&limits, "t", 100).is_ok());
        assert2::assert!(matches!(
            enforcer.check_span_rate(&limits, "t", 100),
            Err(LimitError::IngestionRateExceeded { .. })
        ));
    }

    #[test]
    fn sustained_rate_does_not_exceed_configured_rate_when_burst_is_larger() {
        // rate=100, burst=1000. The configured burst may be absorbed once, but
        // the SUSTAINED rate must track the configured rate, not the larger
        // burst: a steady stream above `rate` must eventually reject rather than
        // sustaining `burst` forever (the old code raised the refill rate to the
        // burst, sustaining 1000/sec indefinitely).
        let enforcer = IngestEnforcer::new();
        let limits = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_spans: 1000,
            ..Limits::default()
        };

        // Drain the one-time burst capacity (1000 spans). With no time to
        // refill, the next 100-span request must reject because the sustained
        // refill is only 100/sec, not the burst.
        for _ in 0..10 {
            assert2::assert!(enforcer.check_span_rate(&limits, "t", 100).is_ok());
        }
        assert2::assert!(matches!(
            enforcer.check_span_rate(&limits, "t", 100),
            Err(LimitError::IngestionRateExceeded { .. })
        ));
    }

    #[test]
    fn rejected_over_limit_request_does_not_starve_later_within_limit_request() {
        // An over-limit request that is rejected must not consume tokens that a
        // subsequent within-limit request needs (all-or-nothing).
        let enforcer = IngestEnforcer::new();
        let limits = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_spans: 100,
            ..Limits::default()
        };

        // Reject an over-limit request (150 > 100 available).
        assert2::assert!(matches!(
            enforcer.check_span_rate(&limits, "t", 150),
            Err(LimitError::IngestionRateExceeded { .. })
        ));
        // A following within-limit request for the full capacity still succeeds
        // because the rejected request consumed nothing.
        assert2::assert!(enforcer.check_span_rate(&limits, "t", 100).is_ok());
    }

    #[test]
    fn search_limit_and_duration_caps() {
        let limits = Limits {
            max_traces_per_search: 1000,
            max_search_duration: hours(1),
            ..Limits::default()
        };

        assert2::assert!(QueryEnforcer::check_search_limit(&limits, 1000).is_ok());
        assert2::assert!(matches!(
            QueryEnforcer::check_search_limit(&limits, 1001),
            Err(LimitError::TracesPerSearchExceeded { .. })
        ));
        let start_ns = 1_000_000_000_000_000_000_i64;
        assert2::assert!(matches!(
            QueryEnforcer::check_search_duration(&limits, start_ns, start_ns + 7_200_000_000_000),
            Err(LimitError::SearchDurationExceeded { .. })
        ));
        assert2::assert!(
            QueryEnforcer::check_search_duration(&limits, start_ns, start_ns + 1_800_000_000_000)
                .is_ok()
        );
    }
}

// === split-modules: generated submodules ===
mod f64_from_u64;
mod ingest_enforcer;
mod query_enforcer;
mod rounded_positive_rate;

use f64_from_u64::f64_from_u64;
pub use ingest_enforcer::IngestEnforcer;
pub use query_enforcer::QueryEnforcer;
use rounded_positive_rate::rounded_positive_rate;
