use super::{
    ActiveLogDeleteFilter, ActiveLogDeleteFilterError, SharedLogDeleteRequests, TimeRange,
    delete_request_time_range, parse_query, ranges_overlap,
};

struct ActiveLogDeleteCandidate<'a> {
    request: &'a super::CompactorDeleteRequest,
    time_range: TimeRange,
}

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
                .map(|time_range| ActiveLogDeleteCandidate {
                    request,
                    time_range,
                })
        })
        .map(|candidate| {
            let query = parse_query(&candidate.request.query).map_err(|source| {
                ActiveLogDeleteFilterError::Parse {
                    query: candidate.request.query.clone(),
                    source,
                }
            })?;
            Ok(ActiveLogDeleteFilter {
                time_range: candidate.time_range,
                query,
            })
        })
        .collect()
}
