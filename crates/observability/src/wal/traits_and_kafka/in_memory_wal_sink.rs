use super::{Arc, LogHotTail, LogWalSink, Mutex, WalLogRecord, WalSinkError, async_trait};

#[derive(Clone, Debug, Default)]
pub struct InMemoryWalSink {
    pub(crate) records: Arc<Mutex<Vec<WalLogRecord>>>,
}

impl InMemoryWalSink {
    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn records(&self) -> Vec<WalLogRecord> {
        self.records.lock().expect("wal sink lock poisoned").clone()
    }
}

#[async_trait]
impl LogWalSink for InMemoryWalSink {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        self.records
            .lock()
            .expect("wal sink lock poisoned")
            .push(record);
        Ok(())
    }
}

impl LogHotTail for InMemoryWalSink {
    fn records(&self) -> Vec<WalLogRecord> {
        InMemoryWalSink::records(self)
    }
}
