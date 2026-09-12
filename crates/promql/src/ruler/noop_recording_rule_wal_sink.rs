use super::{RecordingRuleWalSink, RulerWalError, WalRecord};

pub(crate) struct NoopRecordingRuleWalSink;

#[async_trait::async_trait]
impl RecordingRuleWalSink for NoopRecordingRuleWalSink {
    async fn append_recording_rule_record(&self, _record: WalRecord) -> Result<(), RulerWalError> {
        Ok(())
    }
}
