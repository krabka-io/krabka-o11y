use super::*;

pub(crate) fn optional_usize_param(uri: &Uri, key: &'static str) -> Result<Option<usize>, String> {
    query_param(uri, key)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("invalid query parameter {key}"))
        })
        .transpose()
}
