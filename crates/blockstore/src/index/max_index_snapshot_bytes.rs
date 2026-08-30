use super::*;

/// Maximum byte size of an index snapshot object accepted by [`Index::load`].
///
/// Snapshots come from shared object storage and, per the threat model, may be
/// corrupted or maliciously oversized. A load fully buffers a snapshot in
/// memory before the `serde_json` parse, so an unbounded read could OOM the
/// process. The loader `head()`s the object first and rejects it when it is
/// larger than this cap. This mirrors the `max_decompressed` output cap that
/// the profiles gunzip path uses. The default is 256 MiB, comfortably above a
/// realistic single-tenant-fleet index.
pub const MAX_INDEX_SNAPSHOT_BYTES: ByteSize = mebibytes(256);
