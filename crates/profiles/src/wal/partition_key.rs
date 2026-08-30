use super::Bytes;

/// Produce key for a WAL record: deterministic `(tenant, fingerprint)` bytes.
#[must_use]
pub fn partition_key(tenant: &str, fingerprint: u64) -> Bytes {
    let mut buf = Vec::with_capacity(tenant.len() + 8);
    buf.extend_from_slice(tenant.as_bytes());
    buf.extend_from_slice(&fingerprint.to_be_bytes());
    Bytes::from(buf)
}
