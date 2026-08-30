use super::{HttpQueryError, QuerierState, Time, TimeExt, TimeRange};

pub(crate) fn validate_query_range_limit(
    state: &QuerierState,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let Some(max_query_range) = state.max_query_range else {
        return Ok(());
    };
    // `start_ns` and `end_ns` are instants; only their difference is an extent.
    // The error carries plain nanoseconds so its rendered message is fixed by
    // the `#[error]` format string alone.
    let max_range_ns = max_query_range.nanos_i64();
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or(HttpQueryError::QueryRangeTooLarge {
            range_ns: i64::MAX,
            max_range_ns,
        })?;
    if query_range > max_query_range {
        return Err(HttpQueryError::QueryRangeTooLarge {
            range_ns: query_range.nanos_i64(),
            max_range_ns,
        });
    }
    Ok(())
}
