use super::MAX_INDEX_SHARDS_PER_TENANT;

/// The shard width to cut a tenant spanning `[min_ts, max_ts]` with.
///
/// `requested` is the caller's width. This function doubles it until the span
/// fits [`MAX_INDEX_SHARDS_PER_TENANT`] shards. A width that would overflow is
/// clamped, and the tenant then collapses to a single shard. That is coarse,
/// but it is a correct index and a bounded number of objects.
#[must_use]
pub(crate) fn index_shard_width_for_span(requested: i64, min_ts: i64, max_ts: i64) -> i64 {
    let mut width = requested.max(1);
    loop {
        let first = min_ts.div_euclid(width);
        let last = max_ts.div_euclid(width);
        let shards = u128::from(last.abs_diff(first)).saturating_add(1);
        let ceiling =
            u128::try_from(MAX_INDEX_SHARDS_PER_TENANT).expect("a shard ceiling fits a u128");
        if shards <= ceiling {
            return width;
        }
        let Some(doubled) = width.checked_mul(2) else {
            return i64::MAX;
        };
        width = doubled;
    }
}
