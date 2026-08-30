//! Pyroscope-shaped per-tenant limits for profiles ingest and query paths.

use krabka_units::{ByteSize, Frequency, Time, bytes, convert::TimeExt, hours, per_sec};
use num_traits::ToPrimitive as _;
use serde::{Deserialize, Serialize};

use crate::ids::{EndMs, StartMs};

#[path = "limits/overrides.rs"]
mod overrides;

pub use overrides::{OverridesError, OverridesProvider};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::secs;

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        let limits = Limits::default();

        assert!(
            limits
                == Limits {
                    ingestion_rate: per_sec(10_000),
                    ingestion_burst_profiles: 10_000,
                    max_series: 0,
                    max_label_name: bytes(1024),
                    max_label_value: bytes(2048),
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 2048,
                    max_flamegraph_nodes_max: 0,
                    max_query_length: secs(2_595_600),
                    max_session_id_cardinality: 0,
                }
        );
    }

    #[test]
    fn limit_errors_carry_pyroscope_code_and_status() {
        let rate = LimitError::IngestionRateExceeded {
            rate: 10_000.0,
            observed: 12_000.0,
        };
        assert!(rate.http_status() == 429);
        assert!(rate.connect_code() == "resource_exhausted");

        let name = LimitError::LabelNameTooLong {
            limit: 1024,
            observed: 5000,
        };
        assert!(name.http_status() == 400);
        assert!(name.connect_code() == "invalid_argument");

        let many = LimitError::TooManyLabels {
            limit: 40,
            observed: 41,
        };
        assert!(many.http_status() == 400);

        let duration = LimitError::QueryLengthExceeded {
            limit_secs: 3600,
            observed_secs: 7200,
        };
        assert!(duration.http_status() == 400);

        let cardinality = LimitError::SessionCardinalityExceeded { limit: 1000 };
        assert!(cardinality.http_status() == 429);
    }

    #[test]
    fn limit_error_message_names_the_cap() {
        let value = LimitError::LabelValueTooLong {
            limit: 2048,
            observed: 5000,
        };

        assert!(value.message().contains("2048"));
    }

    #[test]
    fn effective_max_nodes_defaults_and_clamps_like_pyroscope() {
        let limits = Limits {
            max_flamegraph_nodes_default: 2048,
            max_flamegraph_nodes_max: 4096,
            ..Limits::default()
        };

        for (requested, want) in [(0, 2048), (-1, 2048), (1024, 1024), (10_000, 4096)] {
            check!(limits.effective_max_nodes(requested) == want);
        }
    }

    #[test]
    fn validate_query_range_rejects_ranges_above_limit() {
        let limits = Limits {
            max_query_length: secs(60),
            ..Limits::default()
        };

        assert!(
            limits
                .validate_query_range_ms(StartMs(0), EndMs(60_000))
                .is_ok()
        );
        let err = limits
            .validate_query_range_ms(StartMs(0), EndMs(120_000))
            .unwrap_err();
        assert!(
            err == LimitError::QueryLengthExceeded {
                limit_secs: 60,
                observed_secs: 120,
            }
        );
    }

    #[test]
    fn validate_query_range_unlimited_accepts_any_range() {
        // A zero `max_query_length` means unlimited: even a range far larger
        // than any finite cap must be accepted. This pins the `||` in the early
        // return — an `&&` there would fall through and reject against a `0`
        // limit, turning unlimited into "reject everything".
        let limits = Limits {
            max_query_length: <Time as TimeExt>::ZERO,
            ..Limits::default()
        };

        assert!(
            limits
                .validate_query_range_ms(StartMs(0), EndMs(120_000))
                .is_ok()
        );
    }

    #[test]
    fn validate_query_range_rejects_open_ended_ranges_without_overflow() {
        let limits = Limits {
            max_query_length: secs(60),
            ..Limits::default()
        };

        let err = limits
            .validate_query_range_ms(StartMs(0), EndMs(i64::MAX))
            .unwrap_err();

        assert!(matches!(
            err,
            LimitError::QueryLengthExceeded {
                limit_secs: 60,
                observed_secs
            } if observed_secs > 60
        ));
    }
}

// === split-modules: generated submodules ===
mod default_max_query_length;
mod limit_error;
mod limits;

pub use default_max_query_length::DEFAULT_MAX_QUERY_LENGTH;
pub use limit_error::LimitError;
pub use limits::Limits;
