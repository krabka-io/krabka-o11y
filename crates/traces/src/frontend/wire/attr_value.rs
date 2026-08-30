use super::*;

impl From<&AnyValueJson> for AttrValue {
    fn from(v: &AnyValueJson) -> Self {
        match v {
            AnyValueJson::StringValue(s) => AttrValue::Str(s.clone()),
            AnyValueJson::IntValue(i) => AttrValue::Int(i.parse().unwrap_or(0)),
            AnyValueJson::DoubleValue(f) => AttrValue::Float(*f),
            AnyValueJson::BoolValue(b) => AttrValue::Bool(*b),
            // An OTLP array attribute has no single scalar form; project its
            // first scalar (`TraceQL` search attributes are scalar in practice).
            AnyValueJson::ArrayValue(a) => a
                .values
                .first()
                .map_or(AttrValue::Str(String::new()), AttrValue::from),
        }
    }
}
