use super::{Deserialize, OtlpKeyValue};

#[derive(Debug, Deserialize)]
pub(crate) struct OtlpScope {
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) version: String,
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}
