use super::{
    Arc, MetricStore, PrometheusApiState, RulerAlertStateRecord, RulerGroupStateRecord,
    RulerStateSink, RulerWalError,
};

pub struct PrometheusRulerStateSink<S: MetricStore> {
    pub(crate) state: Arc<PrometheusApiState<S>>,
}

impl<S: MetricStore> PrometheusRulerStateSink<S> {
    #[must_use]
    pub fn new(state: Arc<PrometheusApiState<S>>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl<S> RulerStateSink for PrometheusRulerStateSink<S>
where
    S: MetricStore + 'static,
{
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.state.apply_ruler_group_state(record);
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.state.apply_ruler_alert_state(record);
        Ok(())
    }
}
