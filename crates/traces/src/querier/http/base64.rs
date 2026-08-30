use super::*;

pub(crate) fn base64<const N: usize>(bytes: [u8; N]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
