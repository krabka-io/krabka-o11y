
#[derive(Debug, Default)]
pub(crate) struct DiscoveryParams {
    pub(crate) matches: Vec<String>,
    pub(crate) start: Option<String>,
    pub(crate) end: Option<String>,
    pub(crate) limit: Option<usize>,
}
