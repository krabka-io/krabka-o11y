use super::*;

pub(crate) fn parse_positive_time_or_legacy(value: &str, legacy: fn(i64) -> Time) -> Result<Time, String> {
    if let Ok(raw) = value.parse::<i64>() {
        if raw <= 0 {
            return Err("time must be positive".to_owned());
        }
        return Ok(legacy(raw));
    }
    parse::positive_time(value).map_err(|error| error.to_string())
}
