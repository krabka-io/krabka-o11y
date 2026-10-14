use super::{CompactorDeleteRequest, ListDeleteRequestsParams};

pub(crate) fn delete_request_overlaps_filter(
    request: &CompactorDeleteRequest,
    params: &ListDeleteRequestsParams,
) -> bool {
    if let Some(start_time) = params.start_time {
        if let Some(end_time) = params.end_time {
            request.end_time >= start_time && request.start_time <= end_time
        } else {
            true
        }
    } else {
        true
    }
}
