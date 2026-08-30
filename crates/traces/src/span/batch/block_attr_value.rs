use super::*;

pub(crate) fn block_attr_value(value: &AttrValue) -> BlockAttrValue {
    match value {
        AttrValue::Str(value) => BlockAttrValue::Str(vec![value.clone()]),
        AttrValue::Int(value) => BlockAttrValue::Int(vec![*value]),
        AttrValue::Double(value) => BlockAttrValue::Double(vec![*value]),
        AttrValue::Bool(value) => BlockAttrValue::Bool(vec![*value]),
        AttrValue::Bytes(value) => BlockAttrValue::Str(vec![hex::encode(value)]),
    }
}
