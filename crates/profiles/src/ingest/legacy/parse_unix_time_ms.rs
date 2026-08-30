use super::ProfilesError;

pub(crate) fn parse_unix_time_ms(value: &str) -> Result<i64, ProfilesError> {
    let value = value.trim();
    let numeric = value
        .parse::<i64>()
        .map_err(|err| ProfilesError::Invalid(format!("invalid ingest time {value:?}: {err}")))?;
    Ok(if numeric.unsigned_abs() < 10_000_000_000 {
        numeric.saturating_mul(1000)
    } else {
        numeric
    })
}
