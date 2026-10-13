use super::{AttrValue, KeyValue};

pub(crate) fn service_name(attrs: &[KeyValue]) -> String {
    attrs
        .iter()
        .find_map(|kv| {
            if kv.key == "service.name" {
                if let AttrValue::Str(value) = &kv.value {
                    if !value.is_empty() {
                        return Some(value.clone());
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| "unknown_service".to_string())
}
