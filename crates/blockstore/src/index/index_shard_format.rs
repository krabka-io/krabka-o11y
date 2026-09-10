/// Leading bytes of every index shard object.
///
/// A loader reads a shard from shared object storage by key, and a key can
/// name something else. A stale object under a reused prefix and a half-written
/// body both reach the decoder. Four bytes and a version turn that into a
/// decode error that names the shard, rather than an index built from noise.
pub(crate) const INDEX_SHARD_MAGIC: [u8; 4] = *b"KBIX";

/// Version of the shard encoding this build writes and accepts.
///
/// One version, no fallback: Krabka is greenfield, so a shard written by an
/// older build is deleted, not migrated.
pub(crate) const INDEX_SHARD_FORMAT_VERSION: u8 = 1;
