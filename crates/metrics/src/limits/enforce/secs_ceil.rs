use super::{Time, TimeExt, ToPrimitive};

/// An extent in whole seconds, rounded up. This is the unit the limit errors
/// report.
pub(crate) fn secs_ceil(extent: Time) -> u64 {
    extent.secs_f64().ceil().to_u64().unwrap_or(u64::MAX)
}
