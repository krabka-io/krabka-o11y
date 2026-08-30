use super::*;

pub(crate) fn service_name(attrs: &[KeyValue]) -> String {
    attrs
        .iter()
        .find_map(|kv| match (&*kv.key, &kv.value) {
            ("service.name", AttrValue::Str(value)) if !value.is_empty() => Some(value.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "unknown_service".to_string())
}
