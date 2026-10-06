/// A generic attribute value list. Scalars are represented as one-element lists.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AttrValue {
    Str(Vec<String>),
    Int(Vec<i64>),
    Double(#[serde(with = "float_texts")] Vec<f64>),
    Bool(Vec<bool>),
    /// Exact OTLP `AnyValue` JSON for values outside the supported typed query domain.
    Unsupported(String),
}

mod float_texts {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(values: &[f64], serializer: S) -> Result<S::Ok, S::Error> {
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f64>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|value| value.parse().map_err(serde::de::Error::custom))
            .collect()
    }
}
