use super::{HttpQueryError, QuerierState};

/// Applies `Loki`'s `max_entries_limit_per_query` to the request's `limit`
/// parameter.
///
/// A request that names no `limit` is not checked, as in `Loki`: the default
/// the handler then uses is well under any sane cap.
pub(crate) fn validate_query_entries_limit(
    state: &QuerierState,
    limit: Option<usize>,
) -> Result<(), HttpQueryError> {
    let max = state.limits.max_entries_limit_per_query;
    let Some(limit) = limit else {
        return Ok(());
    };
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    if max == 0 || limit <= max {
        return Ok(());
    }
    Err(HttpQueryError::MaxEntriesLimitPerQuery { limit, max })
}
