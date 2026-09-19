use std::sync::Mutex;

use krabka_blockstore::{Labels, TenantId};
use krabka_promql::AlertmanagerAlert;

use super::{
    AlertmanagerSink, RecordingRuleWalSink, RulerAlertStateRecord, RulerGroupStateRecord,
    RulerStateSink, RulerWalError, WalRecord,
};

#[derive(Default)]
pub struct RulerOutputBatch {
    pub wal: Vec<WalRecord>,
    pub alerts: Vec<(Option<TenantId>, Vec<AlertmanagerAlert>)>,
    pub groups: Vec<RulerGroupStateRecord>,
    pub alert_states: Vec<RulerAlertStateRecord>,
}

impl RulerOutputBatch {
    pub fn append(&mut self, mut other: Self) {
        self.wal.append(&mut other.wal);
        self.alerts.append(&mut other.alerts);
        self.groups.append(&mut other.groups);
        self.alert_states.append(&mut other.alert_states);
    }
}

/// Collects one evaluation pass so its Kafka effects can commit under one
/// broker-enforced ruler epoch before any external notification is dispatched.
pub struct BufferedRulerOutputs<'a, A> {
    alert_templates: &'a A,
    batch: Mutex<RulerOutputBatch>,
}

impl<'a, A> BufferedRulerOutputs<'a, A> {
    #[must_use]
    pub fn new(alert_templates: &'a A) -> Self {
        Self {
            alert_templates,
            batch: Mutex::new(RulerOutputBatch::default()),
        }
    }

    #[must_use]
    pub fn into_batch(self) -> RulerOutputBatch {
        self.batch
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl<A: AlertmanagerSink> RecordingRuleWalSink for BufferedRulerOutputs<'_, A> {
    async fn append_recording_rule_record(&self, record: WalRecord) -> Result<(), RulerWalError> {
        self.batch
            .lock()
            .map_err(|_| RulerWalError::Append("ruler output buffer poisoned".to_owned()))?
            .wal
            .push(record);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<A: AlertmanagerSink> AlertmanagerSink for BufferedRulerOutputs<'_, A> {
    fn template_external_labels(&self) -> Labels {
        self.alert_templates.template_external_labels()
    }

    fn template_external_url(&self, alert_name: &str) -> String {
        self.alert_templates.template_external_url(alert_name)
    }

    async fn dispatch_alerts(&self, alerts: Vec<AlertmanagerAlert>) -> Result<(), RulerWalError> {
        self.batch
            .lock()
            .map_err(|_| RulerWalError::Append("ruler output buffer poisoned".to_owned()))?
            .alerts
            .push((None, alerts));
        Ok(())
    }

    async fn dispatch_alerts_for_tenant(
        &self,
        tenant: &TenantId,
        alerts: Vec<AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        self.batch
            .lock()
            .map_err(|_| RulerWalError::Append("ruler output buffer poisoned".to_owned()))?
            .alerts
            .push((Some(tenant.clone()), alerts));
        Ok(())
    }
}

#[async_trait::async_trait]
impl<A: AlertmanagerSink> RulerStateSink for BufferedRulerOutputs<'_, A> {
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.batch
            .lock()
            .map_err(|_| RulerWalError::Append("ruler output buffer poisoned".to_owned()))?
            .groups
            .push(record);
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.batch
            .lock()
            .map_err(|_| RulerWalError::Append("ruler output buffer poisoned".to_owned()))?
            .alert_states
            .push(record);
        Ok(())
    }
}
