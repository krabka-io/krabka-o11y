use super::{KeyValue, AttrValue};

/// Pair an attribute key with the TRUE encoded byte length of its value.
///
/// This measures the real byte length rather than the length of a string form.
/// The `Limits::max_attribute` cap therefore sees the true size of `Bytes`,
/// `Int` and `Double` values. A `String` conversion would mis-report them: a
/// large byte blob would read as length 0 and would bypass the limit.
pub(crate) fn shared_attr_measured(attr: &KeyValue) -> (String, u64) {
    let value_bytes = match &attr.value {
        AttrValue::Str(value) => value.len(),
        AttrValue::Bytes(value) => value.len(),
        AttrValue::Int(value) => value.to_le_bytes().len(),
        AttrValue::Double(value) => value.to_le_bytes().len(),
        AttrValue::Bool(_) => 1,
    };
    (attr.key.clone(), value_bytes as u64)
}
