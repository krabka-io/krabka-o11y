use super::{json, Value, Map};

pub(crate) fn span_status_json(code: i32, message: &str) -> Value {
    // Always emit a status object so the field is never missing (Tempo always
    // sets a Status; Grafana dereferences it). STATUS_CODE_UNSET (0) is the
    // protojson default and is omitted (rendered as `{}`); OK/ERROR are explicit.
    let mut status = Map::new();
    match code {
        1 => {
            status.insert("code".into(), json!("STATUS_CODE_OK"));
        }
        2 => {
            status.insert("code".into(), json!("STATUS_CODE_ERROR"));
        }
        _ => {}
    }
    if !message.is_empty() {
        status.insert("message".into(), json!(message));
    }
    Value::Object(status)
}
