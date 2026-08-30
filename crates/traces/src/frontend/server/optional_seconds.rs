use super::*;

pub(crate) fn optional_seconds(uri: &Uri, key: &str) -> Result<Option<i64>, String> {
    query_param(uri, key)
        .map(|value| {
            parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
        })
        .transpose()
}
