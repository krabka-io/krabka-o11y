use super::{MAX_TENANT_LEN, ProfilesError, is_allowed_byte};

/// Validate a raw tenant id against the Mimir/Pyroscope charset.
///
/// Rejects the following, each with a generic message:
/// - leading or trailing ASCII whitespace (no silent trim),
/// - the empty string,
/// - ids longer than 150 bytes,
/// - the exact segments `"."` and `".."`,
/// - any `/`, `\`, ASCII control byte (`< 0x20` or `0x7f`), or any byte
///   outside the allowed charset.
///
/// On success returns the owned, validated id.
///
/// # Errors
///
/// Returns [`ProfilesError::Invalid`] when `raw` violates any of the rules
/// above.
pub fn validate_tenant(raw: &str) -> Result<String, ProfilesError> {
    let invalid = || ProfilesError::Invalid("invalid tenant id".to_string());

    if raw.is_empty() {
        return Err(invalid());
    }
    // No silent trim: reject leading/trailing ASCII whitespace outright.
    if raw.starts_with(|c: char| c.is_ascii_whitespace())
        || raw.ends_with(|c: char| c.is_ascii_whitespace())
    {
        return Err(invalid());
    }
    if raw.len() > MAX_TENANT_LEN {
        return Err(invalid());
    }
    if raw == "." || raw == ".." {
        return Err(invalid());
    }
    if !raw.bytes().all(is_allowed_byte) {
        return Err(invalid());
    }

    Ok(raw.to_string())
}
