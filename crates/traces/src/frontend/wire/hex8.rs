use super::*;

/// Lowercase hex for an 8-byte span id.
#[must_use]
pub fn hex8(id: &[u8; 8]) -> String {
    hex::encode(id)
}
