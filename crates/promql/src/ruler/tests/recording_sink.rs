use super::*;

#[derive(Default)]
pub(crate) struct RecordingSink {
    pub(crate) records: Mutex<Vec<WalRecord>>,
}

impl RecordingSink {
    pub(crate) fn records(&self) -> Vec<WalRecord> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl super::super::RecordingRuleWalSink for RecordingSink {
    async fn append_recording_rule_record(
        &self,
        record: WalRecord,
    ) -> Result<(), super::super::RulerWalError> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .push(record);
        Ok(())
    }
}
