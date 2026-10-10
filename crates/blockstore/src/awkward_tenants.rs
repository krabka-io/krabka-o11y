//! Tenant names that stress object-key escaping, shared by key-layout tests.

/// Named tenant strings that a key encoder must keep inside one path segment:
/// separators, traversal, glob and quoting characters, and non-ASCII.
pub(crate) const AWKWARD_TENANTS: [(&str, &str); 13] = [
    ("plain", "tenant-a"),
    ("separator", "a/b"),
    ("relative", ".."),
    ("current", "."),
    ("traversal", "../../etc"),
    ("absolute", "/etc/passwd"),
    ("space", "a b"),
    ("star", "a*b"),
    ("marker", "a!b"),
    ("quote", "a'b"),
    ("brackets", "(a)"),
    ("backslash", "a\\b"),
    ("non ASCII", "\u{e9}"),
];
