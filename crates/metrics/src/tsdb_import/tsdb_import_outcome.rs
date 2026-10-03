use super::TsdbImportRecord;

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
