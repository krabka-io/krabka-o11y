use super::MetricStore;

#[derive(Debug, Default)]
pub(crate) struct CardinalityParams {
    pub(crate) selector: Option<String>,
    pub(crate) label_names: Vec<String>,
    pub(crate) limit: Option<usize>,
}
