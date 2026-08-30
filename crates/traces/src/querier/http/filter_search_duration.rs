use super::{SearchResponse, Time};

pub(crate) fn filter_search_duration(
    mut resp: SearchResponse,
    min_duration: Option<Time>,
    max_duration: Option<Time>,
    limit: usize,
) -> SearchResponse {
    if min_duration.is_none() && max_duration.is_none() {
        return resp;
    }
    resp.traces.retain(|trace| {
        min_duration.is_none_or(|min| trace.duration >= min)
            && max_duration.is_none_or(|max| trace.duration <= max)
    });
    resp.inspected_traces = resp.traces.len();
    resp.traces.truncate(limit);
    resp
}
