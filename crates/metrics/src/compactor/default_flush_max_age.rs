use super::*;

/// Default maximum age the oldest buffered WAL record may reach before a flush.
pub const DEFAULT_FLUSH_MAX_AGE: Time = minutes(1);
