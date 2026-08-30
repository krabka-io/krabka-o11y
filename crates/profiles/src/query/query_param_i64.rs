use super::*;

pub(crate) fn query_param_i64(params: &[(String, String)], name: &str) -> Option<i64> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .and_then(|(_, value)| value.parse().ok())
}
