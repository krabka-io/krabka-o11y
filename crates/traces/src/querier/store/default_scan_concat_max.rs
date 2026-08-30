use super::*;

/// Default and maximum safe memory size for a concatenation of scan batches.
pub const DEFAULT_SCAN_CONCAT_MAX: ByteSize = krabka_units::bytes(1_500_000_000);
