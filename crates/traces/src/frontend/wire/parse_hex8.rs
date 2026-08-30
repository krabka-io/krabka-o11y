/// Parse a lowercase-hex 8-byte span id, the lossless inverse of [`hex8`].
#[must_use]
pub fn parse_hex8(s: &str) -> [u8; 8] {
    let mut out = [0u8; 8];
    let _ = hex::decode_to_slice(s, &mut out);
    out
}
