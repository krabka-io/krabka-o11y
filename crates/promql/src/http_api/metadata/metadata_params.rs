use super::*;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct MetadataParams {
    pub(crate) metric: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) limit_per_metric: Option<usize>,
}
