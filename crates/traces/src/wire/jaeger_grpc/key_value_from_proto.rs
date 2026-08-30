use super::*;

pub(crate) fn key_value_from_proto(kv: &api_v2::KeyValue) -> KeyValue {
    let value_type = api_v2::ValueType::try_from(kv.v_type).unwrap_or(api_v2::ValueType::String);
    let value = match value_type {
        api_v2::ValueType::String => AttrValue::Str(kv.v_str.clone()),
        api_v2::ValueType::Bool => AttrValue::Bool(kv.v_bool),
        api_v2::ValueType::Int64 => AttrValue::Int(kv.v_int64),
        api_v2::ValueType::Float64 => AttrValue::Double(kv.v_float64),
        api_v2::ValueType::Binary => AttrValue::Bytes(kv.v_binary.clone()),
    };
    KeyValue {
        key: kv.key.clone(),
        value,
    }
}
