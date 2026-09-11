use super::PATH_ESCAPE_MARKER;

/// Reads back a segment that [`super::escape_object_path_segment`] wrote.
///
/// Returns `None` when `segment` is not one of its results: an escape marker
/// that two uppercase hexadecimal digits do not follow, or bytes that do not
/// spell UTF-8. A reader that gets `None` should treat the object as foreign,
/// which is what the index listings do.
#[must_use]
pub fn unescape_object_path_segment(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        if byte == PATH_ESCAPE_MARKER {
            let high = hex_digit(*bytes.get(index + 1)?)?;
            let low = hex_digit(*bytes.get(index + 2)?)?;
            out.push(high * 16 + low);
            index += 3;
        } else {
            out.push(byte);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
