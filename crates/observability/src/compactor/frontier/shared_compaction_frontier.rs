use super::{Arc, CompactionFrontier, Mutex, WalPosition};

#[derive(Clone, Debug)]
pub struct SharedCompactionFrontier {
    pub(crate) frontier: Arc<Mutex<CompactionFrontier>>,
}

impl SharedCompactionFrontier {
    #[must_use]
    pub fn new(frontier: CompactionFrontier) -> Self {
        Self {
            frontier: Arc::new(Mutex::new(frontier)),
        }
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn snapshot(&self) -> CompactionFrontier {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .clone()
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn advance_partition_offset(&self, position: WalPosition) {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .advance_partition_offset(position);
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn replace(&self, frontier: CompactionFrontier) {
        *self.frontier.lock().expect("frontier mutex poisoned") = frontier;
    }
}

impl Default for SharedCompactionFrontier {
    fn default() -> Self {
        Self::new(CompactionFrontier::new(i64::MIN))
    }
}
