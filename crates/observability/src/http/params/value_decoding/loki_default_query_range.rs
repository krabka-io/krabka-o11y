use super::{Time, hours};

/// Window a query covers when it names neither `start` nor `since`.
pub(crate) const LOKI_DEFAULT_QUERY_RANGE: Time = hours(1);
