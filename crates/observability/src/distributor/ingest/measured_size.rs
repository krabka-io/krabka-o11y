use super::*;

/// A measured length, as a byte quantity.
pub(crate) fn measured_size(len: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(len).unwrap_or(u64::MAX))
}
