//! Lower-case hex rendering of raw ID bytes.
//!
//! Loki structured metadata, OTLP byte attributes and Pyroscope profile IDs
//! all render raw bytes as lower-case hex with no separator. Every crate that
//! renders such bytes goes through [`encode_lower_hex`].

/// Renders `bytes` as lower-case hex, two digits per byte, high nibble first.
#[must_use]
pub fn encode_lower_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn renders_each_byte_as_two_lower_case_digits_high_nibble_first() {
        check!(encode_lower_hex(&[]) == "");
        check!(encode_lower_hex(&[0x0F, 0xAB]) == "0fab");
        check!(encode_lower_hex(&[0xDE, 0xAD, 0xBE, 0xEF]) == "deadbeef");
    }
}
