use super::DiscoveryParams;

/// A `/api/v1/label/{name}/values` request: the label it names and its
/// discovery parameters.
pub(crate) struct LabelValuesQuery {
    pub(crate) name: String,
    pub(crate) params: DiscoveryParams,
}
