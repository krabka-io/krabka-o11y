use super::decode_query_pair;

pub(crate) fn query_param_value(raw_query: Option<&str>, name: &str) -> Option<String> {
    let raw_query = raw_query?;
    for pair in raw_query.split('&') {
        let pair = decode_query_pair(pair).ok()?;
        if pair.key == name {
            return Some(pair.value);
        }
    }
    None
}
