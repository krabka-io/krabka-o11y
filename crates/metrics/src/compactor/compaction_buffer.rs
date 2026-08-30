use super::*;

/// Accumulates polled WAL records across polls so the compactor can flush one
/// larger block per threshold instead of one tiny block per poll.
///
/// The buffer keeps each record's consumer offset and partition metadata, so the
/// flush keys blocks by the buffered offset range exactly as a single-poll write
/// does. The oldest record's arrival time anchors the age-based flush deadline.
pub(crate) struct CompactionBuffer {
    pub(crate) records: Vec<CompactionWalRecord>,
    pub(crate) oldest_arrival: Option<std::time::Instant>,
}

impl CompactionBuffer {
    pub(crate) const fn new() -> Self {
        Self {
            records: Vec::new(),
            oldest_arrival: None,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Appends newly polled records. The first record to enter an empty buffer
    /// anchors the age deadline.
    pub(crate) fn extend(&mut self, records: Vec<CompactionWalRecord>, now: std::time::Instant) {
        if records.is_empty() {
            return;
        }
        if self.oldest_arrival.is_none() {
            self.oldest_arrival = Some(now);
        }
        self.records.extend(records);
    }

    /// Whether the buffer should flush now under the configured thresholds.
    pub(crate) fn should_flush(&self, config: &CompactionLoopConfig, now: std::time::Instant) -> bool {
        if self.is_empty() {
            return false;
        }
        if self.records.len() >= config.flush_max_rows {
            return true;
        }
        self.oldest_arrival
            .is_some_and(|anchor| now.duration_since(anchor).as_time() >= config.flush_max_age)
    }

    /// Takes all buffered records and resets the buffer to empty.
    pub(crate) fn take(&mut self) -> Vec<CompactionWalRecord> {
        self.oldest_arrival = None;
        std::mem::take(&mut self.records)
    }
}
