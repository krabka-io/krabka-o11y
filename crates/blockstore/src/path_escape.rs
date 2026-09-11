//! Escaping of a value into one object-store path segment.
//!
//! A tenant name reaches an object key and an index prefix as a path segment,
//! and it arrives from an untrusted request header. These two functions are
//! what keeps such a name inside its own segment.

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use object_store::path::Path as ObjectPath;

    use super::{escape_object_path_segment, unescape_object_path_segment};

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
            ("separator", "a/b", "a!2Fb"),
            ("backslash", "a\\b", "a!5Cb"),
            ("star", "a*b", "a!2Ab"),
            ("percent", "a%b", "a!25b"),
            ("marker", "a!b", "a!21b"),
            ("control", "a\u{1}b", "a!01b"),
            ("space", "a b", "a!20b"),
            ("non ASCII", "\u{e9}", "!C3!A9"),
            ("traversal", "../other", "..!2Fother"),
            ("trailing traversal", "a/..", "a!2F.."),
        ];

        for (name, raw, escaped) in cases {
            check!(escape_object_path_segment(raw) == escaped, "{name}");
            check!(
                unescape_object_path_segment(escaped) == Some(raw.into()),
                "{name}"
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

mod escape_object_path_segment;
mod path_escape_marker;
mod unescape_object_path_segment;

pub use escape_object_path_segment::escape_object_path_segment;
use path_escape_marker::PATH_ESCAPE_MARKER;
pub use unescape_object_path_segment::unescape_object_path_segment;
