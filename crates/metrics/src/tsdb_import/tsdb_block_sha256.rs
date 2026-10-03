use sha2::{Digest as _, Sha256};

use super::TsdbBlockFiles;

/// The content hash of one uploaded block, as lowercase hex.
///
/// The hash covers the index, every chunk segment in order and the tombstones,
/// each framed by a tag and its length, so that moving bytes from one file to
/// the next changes the hash. It leaves out `meta.json`, whose `thanos` section
/// changes each time a tool touches the block while the samples stay the same.
#[must_use]
pub fn tsdb_block_sha256(files: TsdbBlockFiles<'_>) -> String {
    let mut hasher = Sha256::new();
    let mut frame = |tag: &[u8], bytes: &[u8]| {
        hasher.update(tag);
        hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(bytes);
    };
    frame(b"index", files.index);
    for segment in files.chunk_segments {
        frame(b"chunks", segment);
    }
    if let Some(tombstones) = files.tombstones {
        frame(b"tombstones", tombstones);
    }
    hex::encode(hasher.finalize())
}
