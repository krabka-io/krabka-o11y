use super::{
    HttpQueryError, LOKI_DEFAULT_QUERY_RANGE, QueryKind, QueryParams, TimeExt, TimeRange,
    current_unix_time_ns, optional_start_end_range, start_or_since,
};

pub(crate) fn time_range(
    params: &QueryParams,
    kind: QueryKind,
) -> Result<TimeRange, HttpQueryError> {
    match kind {
        QueryKind::Instant => {
            if let Some(time) = params.time {
                TimeRange::new(time, time).map_err(HttpQueryError::from)
            } else {
                optional_start_end_range(params.start, params.since, params.end)
            }
        }
        QueryKind::Range => {
            let end = params.end.unwrap_or_else(current_unix_time_ns);
            let start = start_or_since(params.start, params.since, Some(end))?
                .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE.nanos_i64()));
            TimeRange::new(start, end).map_err(HttpQueryError::from)
        }
    }
}
