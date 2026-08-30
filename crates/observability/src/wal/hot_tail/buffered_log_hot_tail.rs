use super::*;

#[derive(Clone, Debug, Default)]
pub struct BufferedLogHotTail {
    pub(crate) buffer: Arc<Mutex<HotTailBuffer>>,
}

impl BufferedLogHotTail {
    // Both mutations of this function are permanent survivors, and both are
    // equivalent: `HotTailBuffer::default()` already carries a one-minute
    // width, and the width is an index granularity that push and query both
    // read from the same field. Whatever it is, the two stay consistent and no
    // record is found or lost because of it.
    pub(crate) fn with_bucket_width(bucket_width: Time) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(HotTailBuffer {
                bucket_width,
                ..HotTailBuffer::default()
            })),
        }
    }
    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn records(&self) -> Vec<WalLogRecord> {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .records
            .clone()
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .records_in_range(start_ns, end_ns)
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn append_records(&self, records: Vec<WalLogRecord>) {
        let mut buffer = self.buffer.lock().expect("hot tail buffer lock poisoned");
        for record in records {
            buffer.push(record);
        }
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn prune_compacted(&self, frontier: &CompactionFrontier) -> usize {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .prune_compacted(frontier)
    }
}

impl LogHotTail for BufferedLogHotTail {
    fn records(&self) -> Vec<WalLogRecord> {
        BufferedLogHotTail::records(self)
    }

    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        BufferedLogHotTail::records_in_range(self, start_ns, end_ns)
    }
}
