use super::{ByteSize, mebibytes};

/// Maximum byte size of a profile-index snapshot object accepted by
/// [`ProfileIndex::load`].
///
/// As with [`crate::Index::load`], a load fully buffers a profile-index
/// snapshot in memory before the `serde_json` parse. A corrupt or maliciously
/// oversized object from shared storage could otherwise OOM the process. The
/// loader `head()`s the object first and rejects it above this cap. This
/// mirrors the profiles gunzip `max_decompressed` pattern. The default is
/// 256 MiB.
pub const MAX_PROFILE_INDEX_SNAPSHOT_BYTES: ByteSize = mebibytes(256);
