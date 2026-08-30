use super::*;

pub(crate) fn delete_request_overlaps_filter(
    request: &CompactorDeleteRequest,
    params: &ListDeleteRequestsParams,
) -> bool {
    match (params.start_time, params.end_time) {
        (Some(start_time), Some(end_time)) => {
            request.end_time >= start_time && request.start_time <= end_time
        }
        _ => true,
    }
}
