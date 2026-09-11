use super::{
    HttpQueryError, QuerierState, QueryKind, TimeRange, validate_loki_volume_query_range_limit,
};

pub(crate) fn validate_loki_range_query_range_limit(
    state: &QuerierState,
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if matches!(kind, QueryKind::Range) {
        validate_loki_volume_query_range_limit(state, time_range)?;
    }
    Ok(())
}
