use krabka_domain_macros::EnumName;

use super::{Deserialize, Serialize, StorageFindingSeverity};

/// What an audit found wrong with one object.
///
/// The serialized names are stable. A repair allowlist and a monitoring rule
/// both match on them.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, EnumName,
)]
#[serde(rename_all = "snake_case")]
pub enum StorageFindingKind {
    /// A block that no index names, older than the grace window. No reader
    /// reaches it and no writer will publish it.
    #[name(value = "orphan")]
    Orphan,
    /// A block or sidecar that no index names, inside the grace window. A
    /// writer can still publish it, so a repair never touches it.
    #[name(value = "pending")]
    Pending,
    /// A sidecar whose block is gone, older than the grace window.
    #[name(value = "orphan_sidecar")]
    OrphanSidecar,
    /// A live block whose sidecar is gone.
    #[name(value = "missing_sidecar")]
    MissingSidecar,
    /// An index, manifest or sidecar entry that names an object the store
    /// does not hold.
    #[name(value = "dangling_index_entry")]
    DanglingIndexEntry,
    /// An index entry or manifest that disagrees with where it is stored or
    /// with what it names: an index of one tenant that names a block of
    /// another tenant, or a metrics `.index` manifest that names another
    /// index key, block or tenant than its key.
    #[name(value = "index_mismatch")]
    IndexMismatch,
    /// A block whose Parquet footer or data does not decode.
    #[name(value = "corrupt_block")]
    CorruptBlock,
    /// A sidecar of a live block that does not decode: a profiles `.symdb`
    /// symbol table.
    #[name(value = "corrupt_sidecar")]
    CorruptSidecar,
    /// A block or manifest stamped with a format version this build does not
    /// read.
    #[name(value = "unsupported_format")]
    UnsupportedFormat,
    /// An index manifest that does not decode, or that is absent while blocks
    /// wait on it. A metrics `.index` manifest that does not decode is one.
    #[name(value = "unreadable_manifest")]
    UnreadableManifest,
    /// A delete or erasure request that does not decode.
    #[name(value = "unreadable_delete_state")]
    UnreadableDeleteState,
    /// An index payload whose bytes do not match the checksum its manifest
    /// records.
    #[name(value = "checksum_mismatch")]
    ChecksumMismatch,
    /// Two live blocks of one WAL partition cover overlapping offsets. A
    /// replay after a crash and two writers on one partition both do this.
    #[name(value = "wal_overlap")]
    WalOverlap,
    /// A compaction frontier ahead of every retained block of its partition.
    #[name(value = "stale_frontier")]
    StaleFrontier,
}

impl StorageFindingKind {
    /// Every kind, in serialized-name order.
    pub const ALL: [Self; 14] = [
        Self::ChecksumMismatch,
        Self::CorruptBlock,
        Self::CorruptSidecar,
        Self::DanglingIndexEntry,
        Self::IndexMismatch,
        Self::MissingSidecar,
        Self::Orphan,
        Self::OrphanSidecar,
        Self::Pending,
        Self::StaleFrontier,
        Self::UnreadableDeleteState,
        Self::UnreadableManifest,
        Self::UnsupportedFormat,
        Self::WalOverlap,
    ];

    /// Whether a repair may delete the object. Only an old orphan block and
    /// an old sidecar without its block qualify. Every other kind needs a
    /// person.
    #[must_use]
    pub const fn is_repairable(self) -> bool {
        matches!(self, Self::Orphan | Self::OrphanSidecar)
    }

    /// How much the kind says about the health of the store.
    #[must_use]
    pub const fn severity(self) -> StorageFindingSeverity {
        match self {
            Self::Pending | Self::WalOverlap | Self::StaleFrontier => {
                StorageFindingSeverity::Warning
            }
            _ => StorageFindingSeverity::Damage,
        }
    }
}

impl std::str::FromStr for StorageFindingKind {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == raw)
            .ok_or_else(|| format!("unknown finding kind `{raw}`"))
    }
}
