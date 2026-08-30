use super::Value;

pub(crate) fn tail_frame_is_empty(frame: &Value) -> bool {
    frame
        .get("streams")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty)
}
