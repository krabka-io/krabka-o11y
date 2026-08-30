use super::{Serialize, Deserialize, AnyValueJson};

/// OTLP key/value attribute form. It matches the querier's `attrs_json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValueJson {
    pub key: String,
    pub value: AnyValueJson,
}
