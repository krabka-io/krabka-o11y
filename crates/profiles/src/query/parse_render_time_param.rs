use super::*;

pub(crate) fn parse_render_time_param(
    value: Option<&str>,
    now_ms: NowMs,
    default: DefaultMs,
) -> Result<i64, ProfileError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default.0);
    };
    if value == "now" {
        return Ok(now_ms.0);
    }
    if let Some(offset) = value.strip_prefix("now-") {
        // The offset is an extent; `now` and the resolved bound are instants, so
        // the subtraction happens in epoch milliseconds.
        let resolved = now_ms.0 - parse_render_offset(offset)?.millis_i64();
        return reject_negative_render_time(resolved, value);
    }
    let numeric = value
        .parse::<i64>()
        .map_err(|err| ProfileError::Plan(format!("invalid render time {value:?}: {err}")))?;
    reject_negative_render_time(normalize_render_unix_time(numeric), value)
}
