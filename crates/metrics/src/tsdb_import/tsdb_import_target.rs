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
