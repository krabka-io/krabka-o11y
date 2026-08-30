use super::*;

/// The allowed tenant-ID bytes are ASCII alphanumerics plus `! - _ . * ' ( )`.
pub(crate) fn is_allowed_tenant_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'!' | b'-' | b'_' | b'.' | b'*' | b'\'' | b'(' | b')')
}
