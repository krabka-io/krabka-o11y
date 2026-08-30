use super::{Time, secs};

/// Default time allowed to open a debuginfod connection.
pub const DEFAULT_DEBUGINFOD_CONNECT_TIMEOUT: Time = secs(5);
