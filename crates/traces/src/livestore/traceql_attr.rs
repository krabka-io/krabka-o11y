use super::KeyValue;

pub(crate) fn traceql_attr(attr: &KeyValue) -> Option<krabka_traceql::AttrValue> {
    attr.value.traceql_value()
}
