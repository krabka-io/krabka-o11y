use super::{Time, secs};

/// Longest `delay_for` a tail request may ask the querier to hold back.
pub(crate) const LOKI_MAX_TAIL_DELAY: Time = secs(5);
