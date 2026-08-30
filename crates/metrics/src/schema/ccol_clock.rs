use super::*;

/// Clock reading clock-name column (`Dictionary<Int32, Utf8>`).
///
/// The value names one clock on the host, such as `CLOCK_REALTIME` or
/// `/dev/ptp0`.
pub const CCOL_CLOCK: &str = "clock";
