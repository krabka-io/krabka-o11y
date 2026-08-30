use super::{Serialize, Deserialize, AnyValueJson};

/// OTLP `ArrayValue` body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArrayValueJson {
    #[serde(default)]
    pub values: Vec<AnyValueJson>,
}
