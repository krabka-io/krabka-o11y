use super::{AttrValue, Deserialize, Serialize};

/// One attribute key/value pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: AttrValue,
}

impl KeyValue {
    /// A pair of `key` and `value`.
    #[must_use]
    pub fn new(key: impl Into<String>, value: AttrValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}
