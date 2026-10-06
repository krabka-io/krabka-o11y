use super::{KeyValue, SpanAttr, push_span_attr};

pub(crate) fn event_attrs(attrs: &[KeyValue]) -> Vec<SpanAttr> {
    let mut values = Vec::new();
    for attr in attrs {
        push_span_attr(&mut values, attr.key.clone(), &attr.value);
    }
    values
}
