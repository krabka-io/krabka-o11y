use super::{Time, secs};

/// Default time allowed for a whole debuginfod request, connection included.
pub const DEFAULT_DEBUGINFOD_REQUEST_TIMEOUT: Time = secs(10);
