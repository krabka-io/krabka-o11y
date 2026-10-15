use super::*;

/// The persisted claim that ties one block ULID to one content hash.
///
/// The import creates it before it writes any block object, so a second
/// upload of the same ULID with other content fails before it writes anything.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TsdbImportBinding {
    /// The format version, which is
    /// [`TsdbImportRecord::VERSION`](super::TsdbImportRecord::VERSION).
    pub version: u32,
    pub ulid: String,
    pub sha256: String,
}

/// One Parquet block that an import publishes, with its `.index` manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TsdbImportObject {
    pub kind: MetricBlockKind,
    pub block_key: String,
    pub index_key: String,
    pub rows: u64,
}

/// What [`publish_tsdb_import`](super::publish_tsdb_import) did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TsdbImportOutcome {
    /// This call made the block's objects live.
    Imported(TsdbImportRecord),
    /// The same content was already live. The record is the one that the
    /// first import of that content wrote, so its ULID can differ from the
    /// ULID of this call. Nothing was duplicated.
    AlreadyImported(TsdbImportRecord),
}

/// The persisted record of one imported TSDB block content.
///
/// The import creates it under the content hash after every Parquet object of
/// the block is written. Its creation is the commit point of the import:
/// after it, a retry completes the import and does not roll it back. The
/// import sets [`Self::published`] after it writes every `.index` manifest
/// and the publication marker that makes them live together.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TsdbImportRecord {
    /// The format version. Readers reject any version other than
    /// [`Self::VERSION`] before they act on the record.
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

/// Where one TSDB block import writes, and under which names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsdbImportTarget<'a> {
    pub tenant: &'a str,
    /// The block ULID, in its canonical upper-case form.
    pub block_ulid: &'a str,
    /// The [`tsdb_block_sha256`](super::tsdb_block_sha256) of the uploaded
    /// files.
    pub sha256: &'a str,
    /// The prefix that the metrics manifest loaders list, for example
    /// `metrics`.
    pub manifest_prefix: &'a str,
    /// The prefix that holds the import records, for example
    /// `mimir-block-uploads`.
    pub record_prefix: &'a str,
}
