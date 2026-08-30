use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct RenderQuery {
    pub(crate) query: String,
    pub(crate) from: Option<String>,
    pub(crate) until: Option<String>,
    #[serde(rename = "maxNodes")]
    pub(crate) max_nodes: Option<i64>,
    #[serde(default, rename = "groupBy", deserialize_with = "deserialize_group_by")]
    pub(crate) group_by: Vec<String>,
    pub(crate) format: Option<String>,
}
