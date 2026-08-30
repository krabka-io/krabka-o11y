use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct StackTraceSelectorJson {
    #[serde(default, rename = "callSite")]
    pub(crate) call_site: Vec<StackTraceLocationJson>,
}
