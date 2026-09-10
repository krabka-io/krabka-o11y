use super::{ByteSize, mebibytes};

/// Maximum byte size of one profile-index object accepted by a load.
///
/// A load fully buffers an object in memory before decoding it, so an
/// unbounded read could OOM the process on a corrupt or maliciously oversized
/// object from shared storage. The reader caps the read and rejects anything
/// larger, mirroring the `max_decompressed` output cap that the profiles
/// gunzip path uses.
///
/// The cap is per object, and a published index is a manifest plus one payload
/// per shard, so the bound is tighter than it reads: it was the ceiling on a
/// whole fleet index when the index was one document. The default is 256 MiB,
/// comfortably above a realistic manifest or shard.
pub const MAX_PROFILE_INDEX_SNAPSHOT_BYTES: ByteSize = mebibytes(256);
