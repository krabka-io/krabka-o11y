use super::*;

/// Retention policy for the in-memory WAL tail.
#[derive(Clone, Copy, Debug)]
pub struct RetentionConfig {
    /// Drop samples whose timestamp is older than `newest_ts - max_age`.
    pub max_age: Time,
    /// Drop the oldest records once the store retains more than this many.
    pub max_records: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age: DEFAULT_MAX_AGE,
            max_records: DEFAULT_MAX_RECORDS,
        }
    }
}
