use super::{MillisRange, ProfileError, validate_range};

pub(crate) fn covering_range(ranges: &[MillisRange]) -> Result<MillisRange, ProfileError> {
    let mut start = i64::MAX;
    let mut end = i64::MIN;
    for range in ranges {
        validate_range(range.start_ms, range.end_ms)?;
        start = start.min(range.start_ms);
        end = end.max(range.end_ms);
    }
    Ok(MillisRange::new(start, end))
}
