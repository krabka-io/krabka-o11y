use super::*;

/// The partial result of one metrics job: the series body plus the accounting.
#[derive(Clone, Debug, Default)]
pub struct MetricsPartial {
    pub response: MetricsResponseJson,
    pub metrics: Metrics,
}
