use std::fmt::Write as _;

use super::PATH_ESCAPE_MARKER;

/// Escapes `value` into one object-store path segment.
///
/// Object keys and index prefixes hold a tenant name, and a tenant name
/// arrives in a request header that Krabka does not trust. The result of this
/// function holds only ASCII alphanumerics, `-`, `_`, `.` and the escape
/// marker `!`, so it can never carry a separator, a control byte, or a byte
/// that `object_store` rewrites. Every other byte becomes the marker and two
/// uppercase hexadecimal digits.
///
/// A `value` of exactly `.` or exactly `..` is a relative path segment. It
/// would move the key up a level rather than name a place in it. Those two
/// escape their dots and come back as `!2E` and `!2E!2E`. An interior dot
/// stays a dot, because tenant names hold dots and `a.b` is not a traversal.
///
/// The marker is `!` and not `%`. `object_store` percent-escapes a `%` again
/// when it builds a `Path`, so a `%`-escaped key and the `Path` built from it
/// are two different strings. The shard-payload sweep compares exactly those
/// two strings, and a payload it cannot match is a payload it deletes.
/// `object_store` passes `!` through unchanged.
///
/// The allowed set here is narrower than the one [`crate::TenantId`] accepts.
/// `!`, `*`, `'`, `(` and `)` are valid in a tenant name and still get an
/// escape, so a valid tenant name can change shape on its way into a key. That
/// is deliberate: the mapping is reversible, not an identity, and
/// [`super::unescape_object_path_segment`] is the way back.
#[must_use]
pub fn escape_object_path_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    // A whole-segment `.` or `..` escapes every dot. An escaped dot is `!2E`,
    // so the segment this returns can never be a relative one.
    let escape_every_byte = value == "." || value == "..";
    for byte in value.bytes() {
        if !escape_every_byte
            && (byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            out.push(char::from(byte));
        } else {
            write!(&mut out, "{}{byte:02X}", char::from(PATH_ESCAPE_MARKER))
                .expect("a write to a String never fails");
        }
    }
    out
}
