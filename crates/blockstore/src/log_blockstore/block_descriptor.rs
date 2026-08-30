use super::*;

/// One block's manifest entry: its key, the series it holds, and its on-disk size.
///
/// There is no `Eq`. [`ByteSize`] stores `f64`, so it is only `PartialEq`. A
/// `Vec` holds the descriptors, and `key.object_key()` matches them. Nothing
/// uses a descriptor as a map or set key, so the derive is unused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlockDescriptor {
    pub key: BlockKey,
    pub fingerprints: BTreeSet<SeriesFingerprint>,
    /// Pinned to the manifest's `size_bytes` integer. The JSON encoding is
    /// the on-disk log index format, and it must not move with the in-memory
    /// type.
    #[serde(
        rename = "size_bytes",
        with = "krabka_units::serde_units::numeric::bytes_u64"
    )]
    pub size: ByteSize,
}

impl BlockDescriptor {
    #[must_use]
    pub fn new(key: BlockKey, fingerprints: BTreeSet<SeriesFingerprint>) -> Self {
        Self::new_with_size(key, fingerprints, ByteSize::ZERO)
    }

    #[must_use]
    pub fn new_with_size(
        key: BlockKey,
        fingerprints: BTreeSet<SeriesFingerprint>,
        size: ByteSize,
    ) -> Self {
        Self {
            key,
            fingerprints,
            size,
        }
    }
}
