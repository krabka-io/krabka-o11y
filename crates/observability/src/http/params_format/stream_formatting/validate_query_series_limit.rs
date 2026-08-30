use super::*;

pub(crate) fn validate_query_series_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_series) = state.max_query_series else {
        return Ok(());
    };
    let series = plan.fingerprints.len();
    if series > max_query_series {
        return Err(HttpQueryError::QuerySeriesTooLarge {
            series,
            max_series: max_query_series,
        });
    }
    Ok(())
}
