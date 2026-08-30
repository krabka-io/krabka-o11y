use super::*;

pub(crate) fn query_param_value(raw_query: Option<&str>, name: &str) -> Option<String> {
    let raw_query = raw_query?;
    for pair in raw_query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key).ok()? == name {
            return decode_form_component(value).ok();
        }
    }
    None
}
