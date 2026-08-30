use super::*;

pub(crate) fn metadata_index_range(params: &SeriesParams) -> Result<TimeRange, HttpQueryError> {
    let Some(time_range) = metadata_time_range(params)? else {
        let end_ns = current_unix_time_ns();
        return TimeRange::new(
            end_ns.saturating_sub(LOKI_METADATA_DEFAULT_INDEX_RANGE.nanos_i64()),
            end_ns,
        )
        .map_err(HttpQueryError::from);
    };
    validate_loki_volume_query_range_limit(time_range)?;
    Ok(time_range)
}
