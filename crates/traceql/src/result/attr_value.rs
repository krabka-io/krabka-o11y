/// A typed attribute value.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Float(#[serde(with = "float_text")] f64),
    Bool(bool),
    Array(Vec<AttrValue>),
    /// Exact `AnyValue` JSON, retained for trace retrieval and excluded from typed operands.
    Unsupported(String),
}

// JSON round-tripping must retain NaN/infinities instead of converting to null.
mod float_text {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: impl std::borrow::Borrow<f64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.borrow().to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
