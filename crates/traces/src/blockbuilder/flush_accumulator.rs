use super::*;

/// Accumulates decoded [`PartitionWindow`]s across several WAL polls, so the
/// block-builder can flush one larger block per partition instead of a tiny
/// block per poll.
///
/// The accumulator merges successive polls by partition. It appends their
/// records and widens the inclusive offset range to span every buffered record,
/// so the block object key is a pure function of the buffered offset range.
///
/// A crash-and-reprocess that re-forms the *same* buffer, with the same records
/// and the same flush boundary, gets an identical key, and the re-run
/// overwrites the block idempotently. Flush boundaries depend on timing,
/// through the record count and the age, so this is at-least-once delivery. It
/// does not guarantee byte-identical keys across every recovery.
#[derive(Debug, Default)]
pub struct FlushAccumulator {
    pub(crate) windows: BTreeMap<i32, PartitionWindow>,
    pub(crate) record_count: usize,
    pub(crate) oldest_record_at: Option<Instant>,
}

impl FlushAccumulator {
    /// Create an empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of span records currently buffered across all partitions.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    /// Whether the buffer holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Merge one poll's decoded windows into the buffer.
    ///
    /// This appends records per partition and widens the inclusive offset range
    /// to cover both the buffered and the incoming records. `now` stamps the
    /// arrival time that age-based flushing uses. It is recorded only the first
    /// time records enter an otherwise-empty buffer, so the age tracks the
    /// *oldest* buffered record.
    pub fn merge(&mut self, windows: BTreeMap<i32, PartitionWindow>, now: Instant) {
        for (partition, incoming) in windows {
            if incoming.records.is_empty() {
                continue;
            }
            if self.oldest_record_at.is_none() {
                self.oldest_record_at = Some(now);
            }
            self.record_count += incoming.records.len();
            self.windows
                .entry(partition)
                .and_modify(|buffered| {
                    buffered.offset_range.0 = buffered.offset_range.0.min(incoming.offset_range.0);
                    buffered.offset_range.1 = buffered.offset_range.1.max(incoming.offset_range.1);
                    buffered.records.extend(incoming.records.iter().cloned());
                })
                .or_insert(incoming);
        }
    }

    /// Whether the buffered records should be flushed now.
    ///
    /// This is true once the buffer reaches the record-count threshold, or once
    /// the oldest buffered record ages past `flush_max_age`. It is always false
    /// when the buffer is empty.
    #[must_use]
    pub fn should_flush(&self, config: &BlockBuilderConfig, now: Instant) -> bool {
        if self.record_count == 0 {
            return false;
        }
        if self.record_count >= config.flush_max_records {
            return true;
        }
        match self.oldest_record_at {
            Some(oldest) => now.saturating_duration_since(oldest).as_time() >= config.flush_max_age,
            None => false,
        }
    }

    /// Drain the buffered windows and reset the accumulator to empty.
    #[must_use]
    pub fn take(&mut self) -> BTreeMap<i32, PartitionWindow> {
        self.record_count = 0;
        self.oldest_record_at = None;
        std::mem::take(&mut self.windows)
    }
}
