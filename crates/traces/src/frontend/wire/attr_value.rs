use super::{AnyValueJson, AttrValue};

impl From<&AnyValueJson> for AttrValue {
    fn from(v: &AnyValueJson) -> Self {
        match v {
            AnyValueJson::Unsupported(value) => AttrValue::Unsupported(value.to_string()),
            AnyValueJson::StringValue(s) => AttrValue::Str(s.clone()),
            AnyValueJson::IntValue(i) => AttrValue::Int(i.parse().unwrap_or(0)),
            AnyValueJson::DoubleValue(f) => AttrValue::Float(*f),
            AnyValueJson::BoolValue(b) => AttrValue::Bool(*b),
            AnyValueJson::ArrayValue(array) => {
                AttrValue::Array(array.values.iter().map(AttrValue::from).collect())
            }
        }
    }
}
