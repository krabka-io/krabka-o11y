use super::*;

pub(crate) fn parse_time_or_legacy_i64(
    value: &str,
    legacy_unit: fn(i64) -> Time,
    positive: bool,
) -> Result<Time, String> {
    if let Ok(value) = value.parse::<i64>() {
        if value < 0 || (positive && value == 0) {
            return Err("time must be positive".to_owned());
        }
        return Ok(legacy_unit(value));
    }
    if positive {
        parse::positive_time(value)
    } else {
        parse::non_negative_time(value)
    }
    .map_err(|error| error.to_string())
}
