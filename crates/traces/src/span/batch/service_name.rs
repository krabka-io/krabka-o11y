use super::{AttrValue, KeyValue};

pub(crate) fn service_name(attrs: &[KeyValue]) -> Option<String> {
    attrs.iter().find_map(|attr| {
        (attr.key == "service.name").then(|| match &attr.value {
            AttrValue::Str(value) => Some(value.clone()),
            _ => None,
        })?
    })
}
