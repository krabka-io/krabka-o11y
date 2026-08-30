/// A long pure-hex string, such as a trace or span id, a digest, or a dash-less
/// UUID. The length floor keeps short hex-looking words such as `face` and
/// `cafe` out of the templatize path.
pub(crate) fn is_hex_id(value: &str) -> bool {
    value.len() >= 16 && value.bytes().all(|b| b.is_ascii_hexdigit())
}
