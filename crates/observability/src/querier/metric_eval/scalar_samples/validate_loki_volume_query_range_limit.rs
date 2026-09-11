use super::{
    HttpQueryError, QuerierState, Time, TimeExt, TimeRange, format_loki_model_duration,
    format_loki_query_length,
};

/// Applies the tenant's `max_query_length`: the widest `[start, end]` a query
/// may cover.
///
/// `Loki` refuses such a query with `ErrQueryTooLong`, and the message here is
/// that text, with the tenant's own limit in it.
pub(crate) fn validate_loki_volume_query_range_limit(
    state: &QuerierState,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let limit = state.limits.max_query_length;
    if limit <= Time::ZERO {
        return Ok(());
    }
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or_else(|| HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(Time::from_nanos(i64::MAX)),
            limit: format_loki_model_duration(limit),
        })?;
    if query_range > limit {
        return Err(HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(query_range),
            limit: format_loki_model_duration(limit),
        });
    }
    Ok(())
}
