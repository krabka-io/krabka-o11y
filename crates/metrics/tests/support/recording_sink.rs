//! An in-memory WAL sink for suites that boot the distributor without a
//! broker.

use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use krabka_metrics::{
    WalRecord,
    distributor::{ProduceError, WalSink},
};

/// In-memory WAL sink. It records every appended `WalRecord` and never touches
/// a broker.
#[derive(Default)]
pub struct RecordingSink {
    records: Mutex<Vec<WalRecord>>,
}

#[async_trait]
impl WalSink for RecordingSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .push(record);
        Ok(())
    }
}

impl RecordingSink {
    pub fn len(&self) -> usize {
        self.records.lock().expect("recording sink poisoned").len()
    }
}
