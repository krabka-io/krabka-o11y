use super::*;

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TypedValueJson {
    #[serde(rename = "type", default)]
    pub(crate) type_: String,
    #[serde(default)]
    pub(crate) value: String,
}
