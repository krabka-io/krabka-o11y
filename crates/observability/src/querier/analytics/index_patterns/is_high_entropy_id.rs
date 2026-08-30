/// A long opaque identifier, such as a session token, a base62 id, or an api
/// key. It is purely alphanumeric with both letters and digits. The required
/// digit keeps long lowercase or mixed-case words out of the templatize path,
/// and the punctuation exclusion keeps module paths and file locations
/// intact.
pub(crate) fn is_high_entropy_id(value: &str) -> bool {
    value.len() >= 16
        && value.bytes().all(|b| b.is_ascii_alphanumeric())
        && value.bytes().any(|b| b.is_ascii_digit())
        && value.bytes().any(|b| b.is_ascii_alphabetic())
}
