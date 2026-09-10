use super::{ByteSize, mebibytes};

/// Maximum byte size of one index shard object accepted by a load.
///
/// Shards come from shared object storage and, per the threat model, may be
/// corrupted or maliciously oversized. A load fully buffers a shard in memory
/// before decoding it, so an unbounded read could OOM the process. The reader
/// caps the read and rejects anything larger, mirroring the `max_decompressed`
/// output cap that the profiles gunzip path uses.
///
/// The cap is per shard, and an index is many shards, so the bound is tighter
/// than it reads: it was the ceiling on a whole fleet index when the index was
/// one document. The default is 256 MiB, comfortably above a realistic shard.
pub const MAX_INDEX_SNAPSHOT_BYTES: ByteSize = mebibytes(256);
