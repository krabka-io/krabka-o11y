use super::{PROFILE_SHARD_FORMAT_VERSION, PROFILE_SHARD_MAGIC, ProfileShard, push_uvarint};

/// Encodes one tenant's slot of a profile index as a shard payload.
///
/// The bulk is the shared index's own compact shard encoding, embedded whole
/// rather than reimplemented: the block records, the series and the postings
/// mean the same thing here as on the metrics path, and a second encoding of
/// them would be a second thing to keep correct. What is appended is the part
/// the shared index has no room for, the per-block stacktrace partitions.
pub(crate) fn encode_profile_shard(tenant: &str, shard: &ProfileShard) -> Vec<u8> {
    let inner = shard.index.encode_tenant_as_shard(tenant);

    let mut out = Vec::with_capacity(inner.len() + 64);
    out.extend_from_slice(&PROFILE_SHARD_MAGIC);
    out.push(PROFILE_SHARD_FORMAT_VERSION);
    push_len(&mut out, inner.len());
    out.extend_from_slice(&inner);

    push_len(&mut out, shard.partitions.len());
    for (object_key, partitions) in &shard.partitions {
        push_len(&mut out, object_key.len());
        out.extend_from_slice(object_key.as_bytes());
        push_len(&mut out, partitions.len());
        for partition in partitions {
            push_uvarint(&mut out, *partition);
        }
    }
    out
}

fn push_len(out: &mut Vec<u8>, len: usize) {
    push_uvarint(
        out,
        u64::try_from(len).expect("a length in memory fits a u64"),
    );
}
