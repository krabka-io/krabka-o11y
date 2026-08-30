use super::*;

/// Default retention horizon for the in-memory WAL tail. The store drops
/// samples older than this horizon, measured from the newest sample it has
/// seen, so the hot store cannot grow without bound.
pub(crate) const DEFAULT_MAX_AGE: Time = hours(6);
