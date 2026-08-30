use super::*;

#[derive(Debug)]
pub(crate) struct ConsumerRecordAccumulator {
    pub(crate) records: Vec<ConsumerRecord>,
    pub(crate) oldest_record_at: Option<Instant>,
    pub(crate) flush_records: usize,
    pub(crate) flush_max_age: Time,
}

impl ConsumerRecordAccumulator {
    pub(crate) fn new(flush_records: usize, flush_max_age: Time) -> Self {
        Self {
            records: Vec::new(),
            oldest_record_at: None,
            flush_records: flush_records.max(1),
            flush_max_age,
        }
    }

    pub(crate) fn push(&mut self, mut records: Vec<ConsumerRecord>, now: Instant) {
        if records.is_empty() {
            return;
        }
        self.oldest_record_at.get_or_insert(now);
        self.records.append(&mut records);
    }

    pub(crate) fn should_flush(&self, now: Instant) -> bool {
        if self.records.is_empty() {
            return false;
        }
        if self.records.len() >= self.flush_records {
            return true;
        }
        self.oldest_record_at.is_some_and(|oldest| {
            now.saturating_duration_since(oldest).as_time() >= self.flush_max_age
        })
    }

    pub(crate) fn take(&mut self) -> Vec<ConsumerRecord> {
        self.oldest_record_at = None;
        std::mem::take(&mut self.records)
    }
}
