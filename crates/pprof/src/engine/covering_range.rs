use super::{ProfileError, validate_range};

pub(crate) fn covering_range(ranges: &[(i64, i64)]) -> Result<(i64, i64), ProfileError> {
    let mut start = i64::MAX;
    let mut end = i64::MIN;
    for (range_start, range_end) in ranges {
        validate_range(*range_start, *range_end)?;
        start = start.min(*range_start);
        end = end.max(*range_end);
    }
    Ok((start, end))
}
