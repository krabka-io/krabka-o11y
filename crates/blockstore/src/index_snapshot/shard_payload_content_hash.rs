use super::xxh3_128;

/// Content hash that names a shard payload object.
///
/// The name being a function of the bytes is what makes a payload immutable
/// and a write idempotent: two writers that encode the same shard mint the
/// same key and their puts are interchangeable, while a writer that changed
/// the shard mints a different key and cannot clobber the payload an older
/// retained manifest still names.
///
/// The hash is 128 bits rather than 64 because a collision here does not
/// corrupt one read, it silently serves one shard's index as another's. At a
/// hundred million live payloads, 128 bits leaves a collision probability
/// around 1e-23; 64 bits would leave it around 3e-4.
#[must_use]
pub(crate) fn shard_payload_content_hash(bytes: &[u8]) -> String {
    format!("{:032x}", xxh3_128(bytes))
}
