use super::*;

/// Returns `true` if `byte` is an allowed tenant character.
///
/// Allowed: `A-Z a-z 0-9` and the punctuation `! _ * ' ( ) - .`.
pub(crate) fn is_allowed_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'!' | b'_' | b'*' | b'\'' | b'(' | b')' | b'-' | b'.')
}
