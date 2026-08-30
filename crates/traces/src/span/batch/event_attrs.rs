use super::*;

pub(crate) fn event_attrs(attrs: &[KeyValue]) -> Vec<(String, String)> {
    attrs
        .iter()
        .map(|attr| (attr.key.clone(), event_attr_value(&attr.value)))
        .collect()
}
