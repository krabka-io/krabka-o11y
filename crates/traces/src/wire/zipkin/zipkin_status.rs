use super::{BTreeMap, StatusCode, WireSpanStatus};

pub(crate) fn zipkin_status(tags: &BTreeMap<String, String>) -> WireSpanStatus {
    match tags.get("error") {
        Some(value) => {
            let message = if value == "true" || value == "false" {
                String::new()
            } else {
                value.clone()
            };
            WireSpanStatus {
                code: StatusCode::Error,
                message,
            }
        }
        None => WireSpanStatus {
            code: StatusCode::Unset,
            message: String::new(),
        },
    }
}
