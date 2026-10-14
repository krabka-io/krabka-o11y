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
