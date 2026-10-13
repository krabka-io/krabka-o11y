use super::{DefaultMs, NowMs, ProfileError, QueryParameter, parse_render_time_param};

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
