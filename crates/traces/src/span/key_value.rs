use super::{AttrValue, Deserialize, Serialize};

/// One attribute key/value pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: AttrValue,
}
