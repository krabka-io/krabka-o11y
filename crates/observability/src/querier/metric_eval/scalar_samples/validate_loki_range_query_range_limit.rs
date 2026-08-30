use super::*;

pub(crate) fn validate_loki_range_query_range_limit(
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if matches!(kind, QueryKind::Range) {
        validate_loki_volume_query_range_limit(time_range)?;
    }
    Ok(())
}
