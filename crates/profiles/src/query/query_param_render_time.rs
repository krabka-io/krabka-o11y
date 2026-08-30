use super::*;

pub(crate) fn query_param_render_time(
    params: &[(String, String)],
    name: &str,
    now_ms: NowMs,
    default: DefaultMs,
) -> Result<i64, ProfileError> {
    let value = params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str());
    parse_render_time_param(value, now_ms, default)
}
