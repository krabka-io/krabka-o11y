use super::TsdbImportRecord;

/// How the creation of the import record ended.
pub enum ImportCommit {
    /// This import created the record.
    Won(TsdbImportRecord),
    /// Another import of the same content created the record first.
    Lost(TsdbImportRecord),
}
