use super::{LimitError, Limits, Time, TimeExt, extent_between, secs_ceil};

#[derive(Debug, Default)]
pub struct QueryEnforcer;

impl QueryEnforcer {
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn check_range(
        limits: &Limits,
        start_ms: i64,
        end_ms: i64,
        now_ms: i64,
    ) -> Result<(), LimitError> {
        let length_cap = limits.max_query_length;
        if length_cap > Time::ZERO {
            let span = extent_between(start_ms, end_ms);
            if span > length_cap {
                return Err(LimitError::QueryRangeTooLong {
                    limit_secs: secs_ceil(length_cap),
                    observed_secs: secs_ceil(span),
                });
            }
        }

        let lookback_cap = limits.max_query_lookback;
        if lookback_cap > Time::ZERO {
            let lookback = extent_between(start_ms, now_ms);
            if lookback > lookback_cap {
                return Err(LimitError::QueryLookbackExceeded {
                    limit_secs: secs_ceil(lookback_cap),
                    observed_secs: secs_ceil(lookback),
                });
            }
        }

        Ok(())
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn check_series_count(limits: &Limits, selected: u64) -> Result<(), LimitError> {
        if limits.max_fetched_series_per_query != 0
            && selected > limits.max_fetched_series_per_query
        {
            Err(LimitError::SeriesPerQueryExceeded {
                limit: limits.max_fetched_series_per_query,
                observed: selected,
            })
        } else {
            Ok(())
        }
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn check_sample_count(limits: &Limits, processed: u64) -> Result<(), LimitError> {
        if limits.max_samples_per_query != 0 && processed > limits.max_samples_per_query {
            Err(LimitError::SamplesPerQueryExceeded {
                limit: limits.max_samples_per_query,
                observed: processed,
            })
        } else {
            Ok(())
        }
    }
}
