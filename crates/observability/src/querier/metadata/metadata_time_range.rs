use super::{
    HttpQueryError, SeriesParams, TimeRange, current_unix_time_ns, optional_start_end_range,
};

pub(crate) fn metadata_time_range(
    params: &SeriesParams,
) -> Result<Option<TimeRange>, HttpQueryError> {
    if params.start.is_none() && params.end.is_none() && params.since.is_none() {
        return Ok(None);
    }

    let end = if params.start.is_none() && params.since.is_some() && params.end.is_none() {
        Some(current_unix_time_ns())
    } else {
        params.end
    };
    optional_start_end_range(params.start, params.since, end).map(Some)
}
