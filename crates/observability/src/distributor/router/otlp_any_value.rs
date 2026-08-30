use super::*;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum OtlpAnyValue {
    #[serde(rename = "stringValue")]
    String(String),
    #[serde(rename = "boolValue")]
    Bool(bool),
    #[serde(rename = "intValue")]
    Int(Value),
    #[serde(rename = "doubleValue")]
    Double(Value),
    #[serde(rename = "bytesValue")]
    Bytes(String),
    #[serde(rename = "arrayValue")]
    Array(OtlpArrayValue),
    #[serde(rename = "kvlistValue")]
    Kvlist(OtlpKeyValueList),
}
