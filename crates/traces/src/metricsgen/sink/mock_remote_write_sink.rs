use super::{async_trait, Arc, Mutex, SeriesPayload, RemoteWriteSink, SinkError};

/// Deterministic sink mock that records successful writes.
#[derive(Clone, Default)]
pub struct MockRemoteWriteSink {
    pub(crate) writes: Arc<Mutex<Vec<SeriesPayload>>>,
    pub(crate) fail_next: Arc<Mutex<bool>>,
    pub(crate) fail_after_successes: Arc<Mutex<Option<usize>>>,
}

impl MockRemoteWriteSink {
    /// Configure the next write to fail.
    ///
    /// # Panics
    /// Panics if the mock sink mutex is poisoned.
    pub fn fail_next(&self) {
        *self.fail_next.lock().expect("mock sink mutex poisoned") = true;
    }

    /// Configure the sink to fail after the requested successful writes.
    ///
    /// # Panics
    /// Panics if the mock sink mutex is poisoned.
    pub fn fail_after_successes(&self, successes: usize) {
        *self
            .fail_after_successes
            .lock()
            .expect("mock sink mutex poisoned") = Some(successes);
    }

    /// Return all recorded writes.
    ///
    /// # Panics
    /// Panics if the mock sink mutex is poisoned.
    #[must_use]
    pub fn writes(&self) -> Vec<SeriesPayload> {
        self.writes
            .lock()
            .expect("mock sink mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl RemoteWriteSink for MockRemoteWriteSink {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError> {
        {
            let mut fail_next = self.fail_next.lock().expect("mock sink mutex poisoned");
            if *fail_next {
                *fail_next = false;
                return Err(SinkError::Transport("forced mock failure".into()));
            }
        }
        {
            let successful_writes = self.writes.lock().expect("mock sink mutex poisoned").len();
            let mut fail_after = self
                .fail_after_successes
                .lock()
                .expect("mock sink mutex poisoned");
            if fail_after.is_some_and(|limit| successful_writes >= limit) {
                *fail_after = None;
                return Err(SinkError::Transport("forced mock failure".into()));
            }
        }

        self.writes
            .lock()
            .expect("mock sink mutex poisoned")
            .push(payload.clone());
        Ok(())
    }
}
