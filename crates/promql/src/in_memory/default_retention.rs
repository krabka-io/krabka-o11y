use super::{Time, hours};

/// Default head retention window: the head keeps six hours of samples hot.
pub const DEFAULT_RETENTION: Time = hours(6);
