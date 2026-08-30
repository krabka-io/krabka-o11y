use super::*;

pub(crate) fn parse_positive_time_or_nanos_f64(value: &str) -> Result<Time, String> {
    value.parse::<f64>().map_or_else(
        |_| parse::positive_time(value).map_err(|error| error.to_string()),
        |value| {
            if value.is_finite() && value > 0.0 {
                Ok(Time::from_secs_f64(value / 1_000_000_000.0))
            } else {
                Err("time must be finite and positive".to_owned())
            }
        },
    )
}
