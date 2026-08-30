
/// Percent-escapes a tenant id for use as a single object-store path segment.
///
/// An interior `.` is allowed, because tenant ids legitimately contain dots. A
/// tenant that is *exactly* `.` or `..` would form a path-traversal segment in
/// the object key. Validation happens upstream, so this is defense in depth.
/// This function percent-escapes the dots of a whole-segment `.` or `..`, so the
/// resulting segment can never be a relative-path component. The `kind` and
/// offset segments are formatted separately and never pass through here.
pub(crate) fn escape_object_path_segment(value: &str) -> String {
    // Reject a tenant segment that is exactly `.` or `..` by escaping the dots,
    // which cannot otherwise be produced (an escaped dot is `%2E`, not `.`).
    if value == "." || value == ".." {
        return value.bytes().map(|_| "%2E").collect();
    }
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            out.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(&mut out, "%{byte:02X}").expect("write to String");
        }
    }
    out
}
