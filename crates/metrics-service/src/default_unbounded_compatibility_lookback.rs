use super::{Time, hours};

/// Lookback that replaces an unbounded `i64::MIN..i64::MAX` query range.
///
/// With this lookback, metadata-style requests do not force a full
/// cold-manifest scan.
pub const DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK: Time = hours(1);
