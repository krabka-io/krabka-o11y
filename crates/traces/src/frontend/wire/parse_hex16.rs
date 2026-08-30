use super::*;

/// Parse a lowercase-hex 16-byte trace id, the lossless inverse of [`hex16`].
#[must_use]
pub fn parse_hex16(s: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let _ = hex::decode_to_slice(s, &mut out);
    out
}
