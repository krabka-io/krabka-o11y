use super::{
    ActiveLogDeleteFilter, HttpQueryError, QuerierState, TimeRange,
    active_log_delete_filters_from_requests,
};

pub(crate) fn active_log_delete_filters(
    state: &QuerierState,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<ActiveLogDeleteFilter>, HttpQueryError> {
    let Some(delete_requests) = &state.delete_requests else {
        return Ok(Vec::new());
    };
    Ok(active_log_delete_filters_from_requests(
        delete_requests,
        tenant,
        query_range,
    )?)
}
