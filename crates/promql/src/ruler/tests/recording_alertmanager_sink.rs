use super::*;

#[derive(Default)]
pub(crate) struct RecordingAlertmanagerSink {
    pub(crate) alerts: Mutex<Vec<super::super::AlertmanagerAlert>>,
}

impl RecordingAlertmanagerSink {
    pub(crate) fn alerts(&self) -> Vec<super::super::AlertmanagerAlert> {
        self.alerts
            .lock()
            .expect("alertmanager sink poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl super::super::AlertmanagerSink for RecordingAlertmanagerSink {
    async fn dispatch_alerts(
        &self,
        alerts: Vec<super::super::AlertmanagerAlert>,
    ) -> Result<(), super::super::RulerWalError> {
        self.alerts
            .lock()
            .expect("alertmanager sink poisoned")
            .extend(alerts);
        Ok(())
    }
}
