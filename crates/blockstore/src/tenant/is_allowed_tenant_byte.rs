/// Reports whether `byte` is allowed in a tenant id.
///
/// The set is ASCII alphanumerics plus `! - _ . * ' ( )`, which is what
/// Grafana Mimir's `tenant.ValidTenantID` allows.
pub(super) fn is_allowed_tenant_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'!' | b'-' | b'_' | b'.' | b'*' | b'\'' | b'(' | b')')
}
