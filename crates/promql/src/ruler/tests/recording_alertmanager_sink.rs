use super::*;

#[derive(Default)]
pub(crate) struct RecordingAlertmanagerSink {
    pub(crate) alerts: Mutex<Vec<super::super::AlertmanagerAlert>>,
    pub(crate) external_labels: Labels,
    pub(crate) external_url: String,
}

impl RecordingAlertmanagerSink {
    pub(crate) fn alerts(&self) -> Vec<super::super::AlertmanagerAlert> {
        self.alerts
            .lock()
            .expect("alertmanager sink poisoned")
            .clone()
    }

    pub(crate) fn with_template_context(external_labels: Labels, external_url: &str) -> Self {
        Self {
            external_labels,
            external_url: external_url.to_string(),
            ..Self::default()
        }
    }
}

#[async_trait::async_trait]
impl super::super::AlertmanagerSink for RecordingAlertmanagerSink {
    fn template_external_labels(&self) -> Labels {
        self.external_labels.clone()
    }

    fn template_external_url(&self, _alert_name: &str) -> String {
        self.external_url.clone()
    }

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
