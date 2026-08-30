use super::{RulerAlertStateRecord, RulerGroupStateRecord, RulerStateSink, RulerWalError};

pub struct RulerStateFanoutSink<A, B> {
    pub(crate) first: A,
    pub(crate) second: B,
}

impl<A, B> RulerStateFanoutSink<A, B> {
    #[must_use]
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

#[async_trait::async_trait]
impl<A, B> RulerStateSink for RulerStateFanoutSink<A, B>
where
    A: RulerStateSink,
    B: RulerStateSink,
{
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.first.persist_ruler_group_state(record.clone()).await?;
        self.second.persist_ruler_group_state(record).await
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.first.persist_ruler_alert_state(record.clone()).await?;
        self.second.persist_ruler_alert_state(record).await
    }
}
