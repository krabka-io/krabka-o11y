use super::{is_hex_id, is_high_entropy_id, is_uuid};

/// Whether a token looks like variable data, that is data that varies per line,
/// which the pattern should templatize to `<_>` instead of keeping as a
/// constant part.
///
/// The leading-digit and float checks catch timestamps and other
/// numeric-leading values. Identifiers that begin with a letter, such as
/// UUIDs, trace and span hashes, and opaque high-entropy tokens, need the
/// explicit shape checks, so they do not each become their own pattern.
pub(crate) fn pattern_value_is_variable(value: &str) -> bool {
    let value = value.trim_matches('"');
    if value.is_empty() {
        return false;
    }
    value.as_bytes().first().is_some_and(u8::is_ascii_digit)
        || value.parse::<f64>().is_ok()
        || is_uuid(value)
        || is_hex_id(value)
        || is_high_entropy_id(value)
}
