/// Leading bytes of every trace-index shard payload.
///
/// A loader reads a payload from shared object storage by key, and a key can
/// name something else. A stale object under a reused prefix and a half-written
/// body both reach the decoder. Four bytes and a version turn that into a
/// decode error that names the object, rather than an index built from noise.
pub(crate) const TRACE_SHARD_MAGIC: [u8; 4] = *b"KBTS";

/// Version of the trace shard encoding this build writes and accepts.
///
/// One version, no fallback: Krabka is greenfield, so a payload written by an
/// older build is deleted, not migrated.
pub(crate) const TRACE_SHARD_FORMAT_VERSION: u8 = 1;
