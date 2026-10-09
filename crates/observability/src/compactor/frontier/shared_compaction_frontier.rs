use super::{Arc, CompactionFrontier, Mutex, WalPosition};

#[derive(Clone, Debug)]
pub struct SharedCompactionFrontier {
    pub(crate) frontier: Arc<Mutex<(u64, CompactionFrontier)>>,
}

impl SharedCompactionFrontier {
    #[must_use]
    pub fn new(frontier: CompactionFrontier) -> Self {
        Self {
            frontier: Arc::new(Mutex::new((0, frontier))),
        }
    }

    #[must_use]
    /// # Panics
    /// Panics if the frontier mutex is poisoned.
    pub fn snapshot(&self) -> CompactionFrontier {
        self.snapshot_with_version().1
    }

    pub(crate) fn snapshot_with_version(&self) -> (u64, CompactionFrontier) {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .clone()
    }

    /// # Panics
    /// Panics if the frontier mutex is poisoned or its version counter is exhausted.
    pub fn advance_partition_offset(&self, position: WalPosition) {
        let mut current = self.frontier.lock().expect("frontier mutex poisoned");
        if current
            .1
            .partition_offsets
            .get(&position.partition)
            .is_none_or(|offset| *offset < position.offset)
        {
            current.0 = current
                .0
                .checked_add(1)
                .expect("frontier version exhausted");
            current.1.advance_partition_offset(position);
        }
    }

    /// # Panics
    /// Panics if the frontier mutex is poisoned or its version counter is exhausted.
    pub fn replace(&self, frontier: CompactionFrontier) {
        let mut current = self.frontier.lock().expect("frontier mutex poisoned");
        if current.1 != frontier {
            current.0 = current
                .0
                .checked_add(1)
                .expect("frontier version exhausted");
            current.1 = frontier;
        }
    }
}

impl Default for SharedCompactionFrontier {
    fn default() -> Self {
        Self::new(CompactionFrontier::new(i64::MIN))
    }
}
