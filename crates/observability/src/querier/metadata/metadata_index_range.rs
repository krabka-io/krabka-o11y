use super::{
    HttpQueryError, LOKI_METADATA_DEFAULT_INDEX_RANGE, QuerierState, SeriesParams, TimeExt,
    TimeRange, clamp_query_lookback, current_unix_time_ns, metadata_time_range,
    validate_loki_volume_query_range_limit,
};

pub(crate) fn metadata_index_range(
    state: &QuerierState,
    params: &SeriesParams,
) -> Result<TimeRange, HttpQueryError> {
    let Some(time_range) = metadata_time_range(params)? else {
        let end_ns = current_unix_time_ns();
        return TimeRange::new(
            end_ns.saturating_sub(LOKI_METADATA_DEFAULT_INDEX_RANGE.nanos_i64()),
            end_ns,
        )
        .map_err(HttpQueryError::from);
    };
    let time_range = clamp_query_lookback(&state.limits, time_range, current_unix_time_ns());
    validate_loki_volume_query_range_limit(state, time_range)?;
    Ok(time_range)
}
