use super::{HttpQueryError, OffsetDateTime, Rfc3339, parse_decimal_seconds_timestamp};

pub(crate) fn parse_loki_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(timestamp_ns) = value.parse::<i64>() {
        return Ok(timestamp_ns);
    }

    if let Some(timestamp_ns) = parse_decimal_seconds_timestamp(value) {
        return Ok(timestamp_ns);
    }

    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .and_then(|timestamp| i64::try_from(timestamp.unix_timestamp_nanos()).ok())
        .ok_or_else(|| HttpQueryError::InvalidTimestampQueryParameter {
            name,
            value: value.to_string(),
        })
}
