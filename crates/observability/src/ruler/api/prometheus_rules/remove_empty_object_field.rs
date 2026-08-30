use super::*;

pub(crate) fn remove_empty_object_field(value: &mut Value, field: &'static str) {
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    if fields
        .get(field)
        .and_then(Value::as_object)
        .is_some_and(serde_json::Map::is_empty)
    {
        fields.remove(field);
    }
}
