/// Lowercase hex for a 16-byte trace id.
#[must_use]
pub fn hex16(id: &[u8; 16]) -> String {
    hex::encode(id)
}
