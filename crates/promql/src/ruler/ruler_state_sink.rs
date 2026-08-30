use super::{RulerGroupStateRecord, RulerWalError, RulerAlertStateRecord};

/// Sink for compacted ruler state records.
#[async_trait::async_trait]
pub trait RulerStateSink: Send + Sync {
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError>;

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError>;
}
