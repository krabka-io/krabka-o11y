/// Returns `true` if and only if `build_id` is a valid debuginfod build-id.
///
/// A valid build-id is a non-empty lowercase hex string. debuginfod build-ids
/// are hex digests, so this function rejects a build-id that holds `/`, `.`,
/// `..`, uppercase, or other bytes. The rejection happens before the build-id
/// can go into a URL. This is a defence against SSRF and path traversal.
pub(crate) fn is_valid_build_id(build_id: &str) -> bool {
    build_id.len() >= 2
        && build_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
