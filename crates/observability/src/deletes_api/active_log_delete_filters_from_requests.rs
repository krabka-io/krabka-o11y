use super::*;

pub(crate) fn active_log_delete_filters_from_requests(
    delete_requests: &SharedLogDeleteRequests,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<ActiveLogDeleteFilter>, ActiveLogDeleteFilterError> {
    delete_requests.refresh()?;
    let requests = delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .iter()
        .filter(|request| request.tenant == tenant)
        .filter_map(|request| {
            delete_request_time_range(request)
                .ok()
                .filter(|range| ranges_overlap(*range, query_range))
                .map(|range| (request, range))
        })
        .map(|(request, time_range)| {
            let query = parse_query(&request.query).map_err(|source| {
                ActiveLogDeleteFilterError::Parse {
                    query: request.query.clone(),
                    source,
                }
            })?;
            Ok(ActiveLogDeleteFilter { time_range, query })
        })
        .collect()
}
