//! Escaping of a value into one object-store path segment.
//!
//! A tenant name reaches an object key and an index prefix as a path segment,
//! and it arrives from an untrusted request header. These two functions are
//! what keeps such a name inside its own segment.

use std::fmt::Write as _;

/// The byte that introduces an escape in an object-store path segment.
///
/// [`escape_object_path_segment`] documents why it is `!` and not `%`.
pub(super) const PATH_ESCAPE_MARKER: u8 = b'!';

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
/// [`unescape_object_path_segment`] is the way back.
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

/// Reads back a segment that [`escape_object_path_segment`] wrote.
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

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use object_store::path::Path as ObjectPath;

    use super::{escape_object_path_segment, unescape_object_path_segment};

    struct EscapeCase {
        name: &'static str,
        raw: &'static str,
        escaped: &'static str,
    }

    #[test]
    fn an_ordinary_name_passes_through_unchanged() {
        for name in [
            "tenant-a",
            "team_42",
            "user.name",
            "a",
            "ALL-CAPS",
            "0",
            "...",
        ] {
            check!(escape_object_path_segment(name) == name);
        }
    }

    #[test]
    fn a_relative_path_segment_loses_its_dots() {
        assert!(escape_object_path_segment(".") == "!2E");
        assert!(escape_object_path_segment("..") == "!2E!2E");
        assert!(unescape_object_path_segment("!2E") == Some(".".into()));
        assert!(unescape_object_path_segment("!2E!2E") == Some("..".into()));
    }

    #[test]
    fn every_byte_the_object_store_rewrites_is_escaped_first() {
        let cases = [
            EscapeCase {
                name: "separator",
                raw: "a/b",
                escaped: "a!2Fb",
            },
            EscapeCase {
                name: "backslash",
                raw: "a\\b",
                escaped: "a!5Cb",
            },
            EscapeCase {
                name: "star",
                raw: "a*b",
                escaped: "a!2Ab",
            },
            EscapeCase {
                name: "percent",
                raw: "a%b",
                escaped: "a!25b",
            },
            EscapeCase {
                name: "marker",
                raw: "a!b",
                escaped: "a!21b",
            },
            EscapeCase {
                name: "control",
                raw: "a\u{1}b",
                escaped: "a!01b",
            },
            EscapeCase {
                name: "space",
                raw: "a b",
                escaped: "a!20b",
            },
            EscapeCase {
                name: "non ASCII",
                raw: "\u{e9}",
                escaped: "!C3!A9",
            },
            EscapeCase {
                name: "traversal",
                raw: "../other",
                escaped: "..!2Fother",
            },
            EscapeCase {
                name: "trailing traversal",
                raw: "a/..",
                escaped: "a!2F..",
            },
        ];

        for case in cases {
            check!(
                escape_object_path_segment(case.raw) == case.escaped,
                "{}",
                case.name
            );
            check!(
                unescape_object_path_segment(case.escaped) == Some(case.raw.into()),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn an_escaped_segment_is_what_the_object_store_stores() {
        // `object_store` percent-escapes a segment again on the way into a
        // `Path`. The escape has to survive that, because the shard sweep
        // compares a built key against a listed location byte for byte.
        for raw in ["a/b", "a*b", "..", "a!b", "a'b", "a(b)", "a b", "\u{e9}"] {
            let segment = format!("tenant={}", escape_object_path_segment(raw));
            let path = ObjectPath::from(format!("root/{segment}"));
            check!(path.as_ref() == format!("root/{segment}"), "{raw:?}");
            check!(path.parts().count() == 2, "{raw:?}");
        }
    }

    #[test]
    fn a_segment_the_escape_never_writes_is_not_read_back() {
        for malformed in ["a!", "a!2", "a!zz", "a!2f", "!C3"] {
            check!(
                unescape_object_path_segment(malformed) == None,
                "{malformed}"
            );
        }
    }
}
