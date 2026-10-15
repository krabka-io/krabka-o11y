use super::{DefaultMs, NowMs, ProfileError, parse_render_time_param};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryParameter {
    pub(crate) key: String,
    pub(crate) value: String,
}

pub(crate) fn query_param_i64(params: &[QueryParameter], name: &str) -> Option<i64> {
    params
        .iter()
        .find(|parameter| parameter.key == name)
        .and_then(|parameter| parameter.value.parse().ok())
}

pub(crate) fn query_param_render_time(
    params: &[QueryParameter],
    name: &str,
    now_ms: NowMs,
    default: DefaultMs,
) -> Result<i64, ProfileError> {
    let value = params
        .iter()
        .find(|parameter| parameter.key == name)
        .map(|parameter| parameter.value.as_str());
    parse_render_time_param(value, now_ms, default)
}

/// Rejects a render time bound that resolved to a negative millisecond value.
///
/// A `now-<offset>` larger than `now` gives a negative bound, for example after
/// clock skew or a very long lookback. A literal negative timestamp gives one
/// too. A negative bound is never a valid Unix time, and without this check it
/// travels downstream as a query window edge.
pub(crate) fn reject_negative_render_time(
    resolved_ms: i64,
    raw: &str,
) -> Result<i64, ProfileError> {
    if resolved_ms < 0 {
        return Err(ProfileError::Plan(format!(
            "render time {raw:?} resolves to a negative timestamp"
        )));
    }
    Ok(resolved_ms)
}
