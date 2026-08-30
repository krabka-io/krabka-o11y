use super::*;

pub(crate) fn span_status(tags: &[KeyValue]) -> StatusCode {
    if tags.iter().any(|tag| {
        tag.key == "error"
            && match &tag.value {
                AttrValue::Bool(true) => true,
                AttrValue::Str(value) => value == "true",
                AttrValue::Int(_)
                | AttrValue::Double(_)
                | AttrValue::Bool(false)
                | AttrValue::Bytes(_) => false,
            }
    }) {
        StatusCode::Error
    } else {
        StatusCode::Unset
    }
}
