use super::*;

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ScopeTagsJson {
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) tags: Vec<String>,
}
