use super::{Uri, query_param, parse_seconds_to_ns};

pub(crate) fn required_seconds(uri: &Uri, key: &str) -> Result<i64, String> {
    let Some(value) = query_param(uri, key) else {
        return Err(format!("missing query parameter {key}"));
    };
    parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
}
