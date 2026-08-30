use super::*;

pub(crate) fn add_loki_query_stat_field(target: &mut Value, source: &Value, pointer: &str) {
    let Some(addend) = source.pointer(pointer).and_then(Value::as_u64) else {
        return;
    };
    let Some(current) = target.pointer_mut(pointer) else {
        return;
    };
    let total = current.as_u64().unwrap_or_default().saturating_add(addend);
    *current = json!(total);
}
