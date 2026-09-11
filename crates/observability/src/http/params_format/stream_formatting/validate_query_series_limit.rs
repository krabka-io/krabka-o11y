use super::{HttpQueryError, QuerierState, StreamPlan};

/// Applies `Loki`'s `max_query_series`: the most series one query may match.
pub(crate) fn validate_query_series_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let max_series = state.limits.max_query_series;
    if max_series == 0 {
        return Ok(());
    }
    let series = plan.fingerprints.len();
    if u64::try_from(series).unwrap_or(u64::MAX) > max_series {
        return Err(HttpQueryError::QuerySeriesTooLarge {
            series,
            max_series: usize::try_from(max_series).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}
