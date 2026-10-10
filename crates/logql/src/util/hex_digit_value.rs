/// Returns the value of one ASCII hex digit, in either case.
///
/// Returns `None` for any byte outside `0-9`, `a-f` and `A-F`. This is the
/// digit check behind Go's `url.QueryUnescape`, which both Prometheus
/// `urlquery` templates and Loki form decoding rely on.
#[must_use]
pub const fn hex_digit_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
