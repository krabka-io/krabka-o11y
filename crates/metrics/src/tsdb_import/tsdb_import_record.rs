use super::{Deserialize, Serialize, TsdbImportObject, TsdbImportStats};

/// The persisted record of one imported TSDB block content.
///
/// The import creates it under the content hash after every Parquet object of
/// the block is written. Its creation is the commit point of the import:
/// after it, a retry completes the import and does not roll it back. The
/// import sets [`Self::published`] after it writes every `.index` manifest
/// and the publication marker that makes them live together.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TsdbImportRecord {
    /// The format version. Readers reject a version above [`Self::VERSION`]
    /// before they act on the record.
    pub version: u32,
    /// The ULID of the first upload of this content.
    pub ulid: String,
    /// The [`tsdb_block_sha256`](super::tsdb_block_sha256) of the block.
    pub sha256: String,
    pub stats: TsdbImportStats,
    pub objects: Vec<TsdbImportObject>,
    /// Whether every manifest and the publication marker were written. A
    /// later import of the same content writes the missing objects of an
    /// unpublished record. It leaves a published record as it is, because
    /// compaction and retention delete the manifests of a published import.
    pub published: bool,
}

impl TsdbImportRecord {
    /// The format version of the import records and bindings that this build
    /// writes.
    pub const VERSION: u32 = 1;
}
