use super::*;

pub(crate) fn parse_unix_nano(value: &str) -> Result<UnixNano, String> {
    if value == "max" {
        return Ok(UnixNano(i64::MAX));
    }
    if let Ok(value) = value.parse::<i64>() {
        return Ok(UnixNano(value));
    }
    parse::time(value)
        .map(|value| UnixNano(value.nanos_i64()))
        .map_err(|error| error.to_string())
}
