use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct SampleTypeConfig {
    pub(crate) units: Option<String>,
    #[serde(rename = "display-name")]
    pub(crate) display_name: Option<String>,
    pub(crate) aggregation: Option<String>,
    pub(crate) cumulative: Option<bool>,
    pub(crate) sampled: Option<bool>,
}
