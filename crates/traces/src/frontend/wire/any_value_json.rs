use super::*;

/// OTLP `AnyValue`, holding the variants `TraceQL` surfaces.
///
/// Tempo emits `intValue` as a string and groups multi-valued attributes under
/// `arrayValue`. That matches the querier's `attr_value_json` and
/// `attr_values_json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AnyValueJson {
    #[serde(rename = "stringValue")]
    StringValue(String),
    #[serde(rename = "intValue")]
    IntValue(String),
    #[serde(rename = "doubleValue")]
    DoubleValue(f64),
    #[serde(rename = "boolValue")]
    BoolValue(bool),
    #[serde(rename = "arrayValue")]
    ArrayValue(ArrayValueJson),
}

impl From<&AttrValue> for AnyValueJson {
    fn from(v: &AttrValue) -> Self {
        match v {
            AttrValue::Str(s) => AnyValueJson::StringValue(s.clone()),
            AttrValue::Int(i) => AnyValueJson::IntValue(i.to_string()),
            AttrValue::Float(f) => AnyValueJson::DoubleValue(*f),
            AttrValue::Bool(b) => AnyValueJson::BoolValue(*b),
        }
    }
}
