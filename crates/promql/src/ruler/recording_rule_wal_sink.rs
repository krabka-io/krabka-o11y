use super::{RulerWalError, WalRecord};

/// Sink for recording-rule output records.
#[async_trait::async_trait]
pub trait RecordingRuleWalSink: Send + Sync {
    async fn append_recording_rule_record(&self, record: WalRecord) -> Result<(), RulerWalError>;
}
