use super::{MAX_TENANT_ID_LEN, is_allowed_tenant_byte};

/// Validates a tenant ID against Mimir's `ValidTenantID` rules. It rejects an
/// empty ID, a length over 150 bytes, the reserved `.` and `..` path segments,
/// and any character outside the allowed set of alphanumerics plus
/// `! - _ . * ' ( )`.
///
/// It returns a reason a person can read on rejection.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn validate_tenant(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("tenant ID is empty".to_string());
    }
    if id.len() > MAX_TENANT_ID_LEN {
        return Err(format!(
            "tenant ID is too long: max {MAX_TENANT_ID_LEN} bytes, got {}",
            id.len()
        ));
    }
    if id == "." || id == ".." {
        return Err(format!("tenant ID `{id}` is not allowed"));
    }
    for byte in id.bytes() {
        if !is_allowed_tenant_byte(byte) {
            return Err(format!(
                "tenant ID contains unsupported character `{}`",
                char::from(byte)
            ));
        }
    }
    Ok(())
}
