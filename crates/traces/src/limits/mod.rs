use krabka_units::{ByteSize, Frequency, Time, bytes, convert::TimeExt, hours, per_sec};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod enforce;
mod overrides;

pub use enforce::{IngestEnforcer, QueryEnforcer};
pub use overrides::{OverridesError, OverridesProvider};

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_blockstore::RetentionWindows as _;

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        assert2::assert!(
            Limits::default()
                == Limits {
                    ingestion_rate: per_sec(100_000),
                    ingestion_burst_spans: 100_000,
                    max_spans_per_request: 10_000,
                    max_traces_per_search: 1000,
                    max_spans_per_trace: 200_000,
                    max_attribute: bytes(2048),
                    max_search_duration: <Time as TimeExt>::ZERO,
                    block_retention: hours(336),
                }
        );
    }

    /// Tempo keeps blocks for `336h` unless an operator says otherwise, and a
    /// window of zero keeps them forever. Reading the sentinel the other way
    /// round would delete every block of a tenant that configured nothing.
    #[test]
    fn the_default_block_retention_is_tempos_and_zero_keeps_blocks_forever() {
        check!(Limits::default().block_retention == hours(336));

        let forever = OverridesProvider::new(Limits {
            block_retention: <Time as TimeExt>::ZERO,
            ..Limits::default()
        });
        check!(forever.block_retention("tenant-a") == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn limit_errors_carry_tempo_status() {
        let rate = LimitError::IngestionRateExceeded {
            rate: 100_000.0,
            observed: 120_000.0,
        };
        assert2::assert!(rate.http_status() == 429);

        let big = LimitError::MaxSpansPerTrace {
            limit: 200_000,
            observed: 200_001,
        };
        assert2::assert!(big.http_status() == 400);

        let attr = LimitError::AttributeTooLong {
            limit: 2048,
            observed: 5000,
        };
        assert2::assert!(attr.http_status() == 400);

        let dur = LimitError::SearchDurationExceeded {
            limit_secs: 3600,
            observed_secs: 7200,
        };
        assert2::assert!(dur.http_status() == 400);
    }

    #[test]
    fn limit_error_message_names_the_cap() {
        let big = LimitError::MaxSpansPerTrace {
            limit: 200_000,
            observed: 200_001,
        };

        assert2::assert!(big.message().contains("200000"));
    }
}

mod limit_error;
mod limits_type;

pub use limit_error::LimitError;
pub use limits_type::Limits;
