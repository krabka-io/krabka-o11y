use super::{ByteSize, ByteSizeExt};

/// A Pyroscope label cap of zero means "unlimited". Here that means the
/// function keeps the base tenant limit.
pub(crate) fn positive_or(override_size: ByteSize, base: ByteSize) -> ByteSize {
    if override_size > <ByteSize as ByteSizeExt>::ZERO {
        override_size
    } else {
        base
    }
}
