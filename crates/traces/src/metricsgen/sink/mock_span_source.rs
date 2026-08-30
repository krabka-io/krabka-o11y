use super::{async_trait, Arc, Mutex, VecDeque, SpanRecord, SpanSource, SinkError};

/// Deterministic source mock that returns scripted batches.
#[derive(Clone, Default)]
pub struct MockSpanSource {
    pub(crate) batches: Arc<Mutex<VecDeque<Vec<SpanRecord>>>>,
    pub(crate) commits: Arc<Mutex<usize>>,
}

impl MockSpanSource {
    /// Queue a batch for the next poll.
    ///
    /// # Panics
    /// Panics if the mock source mutex is poisoned.
    pub fn push_batch(&self, batch: Vec<SpanRecord>) {
        self.batches
            .lock()
            .expect("mock source mutex poisoned")
            .push_back(batch);
    }

    /// Return the number of committed batches.
    ///
    /// # Panics
    /// Panics if the mock source mutex is poisoned.
    #[must_use]
    pub fn commits(&self) -> usize {
        *self.commits.lock().expect("mock source mutex poisoned")
    }
}

#[async_trait]
impl SpanSource for MockSpanSource {
    async fn poll(&self, _max: usize) -> Result<Vec<SpanRecord>, SinkError> {
        Ok(self
            .batches
            .lock()
            .expect("mock source mutex poisoned")
            .pop_front()
            .unwrap_or_default())
    }

    async fn commit(&self) -> Result<(), SinkError> {
        *self.commits.lock().expect("mock source mutex poisoned") += 1;
        Ok(())
    }
}
