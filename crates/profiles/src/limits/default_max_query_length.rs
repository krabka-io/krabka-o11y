use super::{Time, hours};

/// Pyroscope's default `max_query_length` (`721h`).
///
/// This matches the upstream Pyroscope default
/// `validation.Limits.MaxQueryLength`, so the querier rejects an unbounded
/// explicit range of `start=0, end=i64::MAX` instead of a scan of the whole
/// store.
pub const DEFAULT_MAX_QUERY_LENGTH: Time = hours(721);
