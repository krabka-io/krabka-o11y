use super::*;

pub(crate) fn delete_request_time_range(
    request: &CompactorDeleteRequest,
) -> Result<TimeRange, ActiveLogDeleteFilterError> {
    let start_ns =
        request
            .start_time
            .checked_mul(1_000_000_000)
            .ok_or(BlockStoreError::InvalidTimeRange {
                start_ns: request.start_time,
                end_ns: request.end_time,
            })?;
    let end_ns = request
        .end_time
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(999_999_999))
        .ok_or(BlockStoreError::InvalidTimeRange {
            start_ns: request.start_time,
            end_ns: request.end_time,
        })?;
    TimeRange::new(start_ns, end_ns).map_err(ActiveLogDeleteFilterError::from)
}
