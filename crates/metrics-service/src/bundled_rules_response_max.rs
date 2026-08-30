use super::{ByteSize, kibibytes};

/// The largest ruler-config response body this reads back for an error message.
pub(crate) const BUNDLED_RULES_RESPONSE_MAX: ByteSize = kibibytes(64);
