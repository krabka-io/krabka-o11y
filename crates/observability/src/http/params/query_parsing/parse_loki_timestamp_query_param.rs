use super::{HttpQueryError, OffsetDateTime, Rfc3339, parse_decimal_seconds_timestamp};

pub(crate) fn parse_loki_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(timestamp) = value.parse::<i64>() {
        return if value.len() <= 10 {
            timestamp.checked_mul(1_000_000_000).ok_or_else(|| {
                HttpQueryError::InvalidTimestampQueryParameter {
                    name,
                    value: value.to_string(),
                }
            })
        } else {
            Ok(timestamp)
        };
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

#[cfg(test)]
mod tests {
    use super::{HttpQueryError, parse_loki_timestamp_query_param};

    #[test]
    fn second_precision_timestamp_that_overflows_nanoseconds_is_rejected() {
        let value = "9999999999";
        let error = parse_loki_timestamp_query_param("time", value)
            .expect_err("the nanosecond timestamp overflows i64");

        assert!(matches!(
            error,
            HttpQueryError::InvalidTimestampQueryParameter {
                name: "time",
                value: rejected,
            } if rejected == value
        ));
    }
}
