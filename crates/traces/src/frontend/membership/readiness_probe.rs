use super::{QuerierHealth, async_trait};

/// How the frontend asks one querier whether it can answer.
///
/// The production implementation is [`HttpReadinessProbe`](super::HttpReadinessProbe),
/// which reads the querier's own `/ready`. The trait exists so a test can put
/// a querier into any health without needing a process that is genuinely in
/// it.
#[async_trait]
pub trait ReadinessProbe: Send + Sync {
    /// Ask `addr` whether it is ready. This never fails: a probe that cannot
    /// complete is itself the answer.
    async fn probe(&self, addr: &str) -> QuerierHealth;
}
