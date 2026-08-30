use super::{Bytes, fnv1_32};

/// The Kafka produce key for traces: `hash(trace_id)`.
#[must_use]
pub fn partition_key(trace_id: &[u8; 16]) -> Bytes {
    Bytes::copy_from_slice(&fnv1_32(trace_id).to_be_bytes())
}
