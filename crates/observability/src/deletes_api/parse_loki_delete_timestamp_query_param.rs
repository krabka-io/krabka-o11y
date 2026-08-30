use super::*;

pub(crate) fn parse_loki_delete_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(seconds) = value.parse::<i64>() {
        return Ok(seconds);
    }
    if let Some(timestamp_ns) = parse_decimal_seconds_timestamp(value) {
        return Ok(timestamp_ns / 1_000_000_000);
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .map(time::OffsetDateTime::unix_timestamp)
        .ok_or_else(|| HttpQueryError::InvalidTimestampQueryParameter {
            name,
            value: value.to_string(),
        })
}
