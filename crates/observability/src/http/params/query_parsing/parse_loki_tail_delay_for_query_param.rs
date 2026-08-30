use super::*;

pub(crate) fn parse_loki_tail_delay_for_query_param(value: &str) -> Result<i64, HttpQueryError> {
    if let Ok(seconds) = value.parse::<i64>() {
        seconds
            .checked_mul(1_000_000_000)
            .ok_or_else(|| HttpQueryError::InvalidQueryParameter {
                name: "delay_for",
                value: value.to_string(),
            })
    } else if let Some(duration_ns) = parse_decimal_seconds_timestamp(value) {
        Ok(duration_ns)
    } else {
        parse_prometheus_duration(value).ok_or_else(|| {
            HttpQueryError::InvalidDurationQueryParameter {
                value: value.to_string(),
            }
        })
    }
}
