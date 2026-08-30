use super::*;

#[derive(Default)]
pub(crate) struct RecordingRulerStateSink {
    pub(crate) group_records: Mutex<Vec<super::super::RulerGroupStateRecord>>,
    pub(crate) alert_records: Mutex<Vec<super::super::RulerAlertStateRecord>>,
}

impl RecordingRulerStateSink {
    pub(crate) fn group_records(&self) -> Vec<super::super::RulerGroupStateRecord> {
        self.group_records
            .lock()
            .expect("ruler state sink poisoned")
            .clone()
    }

    pub(crate) fn alert_records(&self) -> Vec<super::super::RulerAlertStateRecord> {
        self.alert_records
            .lock()
            .expect("ruler state sink poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl super::super::RulerStateSink for RecordingRulerStateSink {
    async fn persist_ruler_group_state(
        &self,
        record: super::super::RulerGroupStateRecord,
    ) -> Result<(), super::super::RulerWalError> {
        self.group_records
            .lock()
            .expect("ruler state sink poisoned")
            .push(record);
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        record: super::super::RulerAlertStateRecord,
    ) -> Result<(), super::super::RulerWalError> {
        self.alert_records
            .lock()
            .expect("ruler state sink poisoned")
            .push(record);
        Ok(())
    }
}
