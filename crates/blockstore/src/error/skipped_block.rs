use super::{BlockSkipReason, Display};

/// One block a scan could not read and left out of its result.
///
/// `detail` is the backend error rendered as text, unlike
/// [`BlockReadFailure`](super::BlockReadFailure), which keeps it typed. The
/// two serve different readers: a caller deciding *what to do* matches on the
/// typed failure, and a caller writing a `warnings` entry into a Loki or Tempo
/// response body needs a sentence. Keeping the report plain also keeps it
/// `Clone` and `PartialEq`, which neither `object_store::Error` nor
/// `ParquetError` is, so a test can compare a whole report at once.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SkippedBlock {
    /// The object key the index named.
    pub object_key: String,

    /// Why the block was left out.
    pub reason: BlockSkipReason,

    /// The backend error, rendered for a human.
    pub detail: String,
}

impl Display for SkippedBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "block `{}` skipped ({}): {}",
            self.object_key, self.reason, self.detail
        )
    }
}
