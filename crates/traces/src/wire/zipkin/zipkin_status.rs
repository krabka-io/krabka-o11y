use super::{BTreeMap, StatusCode};

pub(crate) fn zipkin_status(tags: &BTreeMap<String, String>) -> (StatusCode, String) {
    match tags.get("error") {
        Some(value) => {
            let message = if value == "true" || value == "false" {
                String::new()
            } else {
                value.clone()
            };
            (StatusCode::Error, message)
        }
        None => (StatusCode::Unset, String::new()),
    }
}
