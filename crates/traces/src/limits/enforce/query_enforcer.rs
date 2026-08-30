use super::*;

pub struct QueryEnforcer;

impl QueryEnforcer {
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_search_limit(limits: &Limits, requested: u64) -> Result<(), LimitError> {
        let limit = limits.max_traces_per_search;
        if limit != 0 && requested > limit {
            return Err(LimitError::TracesPerSearchExceeded { limit, requested });
        }
        Ok(())
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_search_duration(
        limits: &Limits,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(), LimitError> {
        let limit_secs = u64::try_from(limits.max_search_duration.secs_i64()).unwrap_or(0);
        if limit_secs == 0 {
            return Ok(());
        }
        let observed = Time::from_nanos(end_ns.saturating_sub(start_ns));
        let observed_secs = observed.secs_f64().ceil().to_u64().unwrap_or(0);
        if observed_secs > limit_secs {
            return Err(LimitError::SearchDurationExceeded {
                limit_secs,
                observed_secs,
            });
        }
        Ok(())
    }
}
