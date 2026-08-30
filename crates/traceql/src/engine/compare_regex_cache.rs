use super::HashMap;

/// Per-query cache of compiled selection regexes, keyed by the raw pattern.
///
/// The key is the raw `=~`/`!~` pattern string. Each value is the pattern
/// compiled as `^(?:pat)$`, which matches how `string_cmp` anchors a pattern.
/// `assemble_compare_response` builds the cache once, and every scanned row
/// reuses it, so a regex selection does not recompile per span. A pattern that
/// does not compile stays absent from the map, and the lookup treats an absent
/// pattern as a non-match. This keeps the earlier best-effort behavior instead
/// of a failed query.
pub(crate) type CompareRegexCache = HashMap<String, regex::Regex>;
