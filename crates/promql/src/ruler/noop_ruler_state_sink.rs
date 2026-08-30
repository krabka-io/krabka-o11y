use super::*;

pub(crate) struct NoopRulerStateSink;

#[async_trait::async_trait]
impl RulerStateSink for NoopRulerStateSink {
    async fn persist_ruler_group_state(
        &self,
        _record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        _record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        Ok(())
    }
}
