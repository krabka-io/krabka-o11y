use super::{Deserialize, Serialize};

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
