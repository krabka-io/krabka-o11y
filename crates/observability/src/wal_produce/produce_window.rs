use super::NonZeroUsize;

/// Records that one request may hold enqueued and unacked.
///
/// The pinned producer defaults to a 16 KiB batch and five in-flight requests
/// per partition, so about 400 records of a typical 200-byte WAL record fill
/// one partition's in-flight budget. A window of 1024 keeps several
/// partitions' budgets full and still caps what a single request queues inside
/// the producer.
pub const DEFAULT_PRODUCE_WINDOW: NonZeroUsize = NonZeroUsize::new(1024).expect("1024 is not zero");

/// The count of records that may be in flight at one time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProduceWindow(NonZeroUsize);

impl ProduceWindow {
    /// Builds a window of `records`.
    #[must_use]
    pub const fn new(records: NonZeroUsize) -> Self {
        Self(records)
    }

    /// Returns the window as a count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for ProduceWindow {
    fn default() -> Self {
        Self(DEFAULT_PRODUCE_WINDOW)
    }
}
