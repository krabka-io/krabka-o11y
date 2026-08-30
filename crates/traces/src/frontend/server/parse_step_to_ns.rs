use super::*;

/// `step` may be bare epoch-seconds OR a Go-duration such as `30s`, `5m` or
/// `100ms`. Grafana's Tempo datasource sends the duration form.
///
/// This mirrors the querier's `parse_step_to_ns`, so the frontend accepts
/// exactly what the querier accepts. Without it, the frontend would `400` a
/// query the querier handles.
pub(crate) fn parse_step_to_ns(value: &str) -> Option<i64> {
    parse_seconds_to_ns(value).or_else(|| i64::try_from(parse_go_duration_ns(value).ok()?).ok())
}
