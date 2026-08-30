use super::*;

pub(crate) fn validate_loki_volume_query_range_limit(
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or_else(|| HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(Time::from_nanos(i64::MAX)),
        })?;
    if query_range > LOKI_VOLUME_MAX_QUERY_RANGE {
        return Err(HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(query_range),
        });
    }
    Ok(())
}
