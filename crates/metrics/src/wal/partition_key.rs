use super::Bytes;

/// Producer key for a tenant and fingerprint pair. The Kafka producer hashes
/// this byte key to choose a partition, which keeps the per-series order.
#[must_use]
pub fn partition_key(tenant: &str, fp: u64) -> Bytes {
    let mut bytes = Vec::with_capacity(tenant.len() + 1 + std::mem::size_of::<u64>());
    bytes.extend_from_slice(tenant.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&fp.to_be_bytes());
    Bytes::from(bytes)
}
