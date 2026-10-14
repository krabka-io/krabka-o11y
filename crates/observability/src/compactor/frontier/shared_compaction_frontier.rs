use super::{Arc, CompactionFrontier, Mutex, WalPosition};

#[derive(Clone, Debug)]
pub(crate) struct SharedFrontierSnapshot {
    pub(crate) version: u64,
    pub(crate) frontier: CompactionFrontier,
}

#[derive(Clone, Debug)]
pub(crate) struct SharedFrontierState {
    version: u64,
    frontier: CompactionFrontier,
}

#[derive(Clone, Debug)]
pub struct SharedCompactionFrontier {
    pub(crate) frontier: Arc<Mutex<SharedFrontierState>>,
}

impl SharedCompactionFrontier {
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.frontier, &other.frontier)
    }

    #[must_use]
    pub fn new(frontier: CompactionFrontier) -> Self {
        Self {
            frontier: Arc::new(Mutex::new(SharedFrontierState {
                version: 0,
                frontier,
            })),
        }
    }

    #[must_use]
    /// # Panics
    /// Panics if the frontier mutex is poisoned.
    pub fn snapshot(&self) -> CompactionFrontier {
        self.snapshot_with_version().frontier
    }

    pub(crate) fn snapshot_with_version(&self) -> SharedFrontierSnapshot {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .clone()
            .into()
    }

    /// # Panics
    /// Panics if the frontier mutex is poisoned or its version counter is exhausted.
    pub fn advance_partition_offset(&self, position: WalPosition) {
        let mut current = self.frontier.lock().expect("frontier mutex poisoned");
        if current
            .frontier
            .partition_offsets
            .get(&position.partition)
            .is_none_or(|offset| *offset < position.offset)
        {
            current.version = current
                .version
                .checked_add(1)
                .expect("frontier version exhausted");
            current.frontier.advance_partition_offset(position);
        }
    }

    /// # Panics
    /// Panics if the frontier mutex is poisoned or its version counter is exhausted.
    pub fn replace(&self, frontier: CompactionFrontier) {
        let mut current = self.frontier.lock().expect("frontier mutex poisoned");
        if current.frontier != frontier {
            current.version = current
                .version
                .checked_add(1)
                .expect("frontier version exhausted");
            current.frontier = frontier;
        }
    }
}

impl From<SharedFrontierState> for SharedFrontierSnapshot {
    fn from(state: SharedFrontierState) -> Self {
        Self {
            version: state.version,
            frontier: state.frontier,
        }
    }
}

impl Default for SharedCompactionFrontier {
    fn default() -> Self {
        Self::new(CompactionFrontier::new(i64::MIN))
    }
}
