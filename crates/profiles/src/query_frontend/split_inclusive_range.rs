use super::{ProfileError, Time, TimeExt};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn split_inclusive_range(
    start_ms: i64,
    end_ms: i64,
    shard_width: Time,
) -> Result<Vec<(i64, i64)>, ProfileError> {
    // The bounds are epoch-millisecond instants and the shard width is an
    // extent, so the width converts once here and the walk stays exact integer
    // arithmetic on instants.
    let shard_width_ms = shard_width.millis_i64();
    if shard_width_ms <= 0 {
        return Err(ProfileError::Plan(format!(
            "query frontend shard width must be positive, got {shard_width_ms}"
        )));
    }
    if start_ms > end_ms {
        return Err(ProfileError::Plan(format!(
            "invalid query range: start {start_ms} is after end {end_ms}"
        )));
    }

    let mut shards = Vec::new();
    let mut current = start_ms;
    while current <= end_ms {
        let shard_end = current.saturating_add(shard_width_ms - 1).min(end_ms);
        shards.push((current, shard_end));
        let Some(next) = shard_end.checked_add(1) else {
            break;
        };
        current = next;
    }
    Ok(shards)
}
