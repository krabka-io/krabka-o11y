use super::*;

pub(crate) fn parse_loki_duration_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    let duration = if let Ok(seconds) = value.parse::<i64>() {
        seconds.checked_mul(1_000_000_000).ok_or_else(|| {
            HttpQueryError::InvalidDurationQueryParameter {
                value: value.to_string(),
            }
        })?
    } else if let Some(duration_ns) = parse_decimal_seconds_timestamp(value) {
        duration_ns
    } else {
        parse_prometheus_duration(value).ok_or_else(|| {
            if name == "since" {
                HttpQueryError::InvalidSinceQueryParameter {
                    value: value.to_string(),
                }
            } else {
                HttpQueryError::InvalidDurationQueryParameter {
                    value: value.to_string(),
                }
            }
        })?
    };

    if name == "since" && duration <= 0 {
        return Err(HttpQueryError::InvalidSinceQueryParameter {
            value: value.to_string(),
        });
    }

    Ok(duration)
}
