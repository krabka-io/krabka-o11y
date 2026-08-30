use super::{Time, hours};

/// Window the metadata endpoints index over when the request names no range.
pub(crate) const LOKI_METADATA_DEFAULT_INDEX_RANGE: Time = hours(6);
