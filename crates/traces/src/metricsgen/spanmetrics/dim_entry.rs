use super::{LatencyHistogram, Exemplar};

#[derive(Clone, Debug)]
pub(crate) struct DimEntry {
    pub(crate) calls: f64,
    pub(crate) size_total: f64,
    pub(crate) latency: LatencyHistogram,
    pub(crate) exemplars: Vec<Exemplar>,
}
