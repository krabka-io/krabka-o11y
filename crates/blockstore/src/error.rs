//! Error type for the block store.

use std::fmt::Display;

use object_store::Error as ObjectStoreError;
use parquet::errors::ParquetError;

#[cfg(test)]
mod tests {
    use super::*;

    struct SkipReasonCase {
        name: &'static str,
        failure: BlockReadFailure,
        expected: Option<BlockSkipReason>,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct SkipReasonObservation {
        name: &'static str,
        reason: Option<BlockSkipReason>,
    }

    fn not_found(path: &str) -> ObjectStoreError {
        ObjectStoreError::NotFound {
            path: path.to_string(),
            source: "no such object".into(),
        }
    }

    fn unreachable_store() -> ObjectStoreError {
        ObjectStoreError::Generic {
            store: "S3",
            source: "connection reset".into(),
        }
    }

    /// The classification is the whole point of the type, so it is worth a
    /// table: each row is a way one block read can fail, and the reason (or
    /// its absence) is the scan's licence to skip that block.
    #[test]
    fn skip_reason_separates_the_blocks_fault_from_the_stores() {
        let cases = [
            SkipReasonCase {
                name: "the object is gone",
                failure: BlockReadFailure::ObjectStore(not_found("b1.parquet")),
                expected: Some(BlockSkipReason::Missing),
            },
            SkipReasonCase {
                name: "the object went while the footer was being read",
                failure: BlockReadFailure::Parquet(ParquetError::External(Box::new(not_found(
                    "b1.parquet",
                )))),
                expected: Some(BlockSkipReason::Missing),
            },
            SkipReasonCase {
                name: "the bytes are not a parquet block",
                failure: BlockReadFailure::Parquet(ParquetError::General(
                    "corrupt footer".to_string(),
                )),
                expected: Some(BlockSkipReason::Corrupt),
            },
            SkipReasonCase {
                name: "the store is unreachable",
                failure: BlockReadFailure::ObjectStore(unreachable_store()),
                expected: None,
            },
            SkipReasonCase {
                name: "the store failed while the footer was being read",
                failure: BlockReadFailure::Parquet(ParquetError::External(Box::new(
                    unreachable_store(),
                ))),
                expected: None,
            },
        ];

        let got = cases
            .iter()
            .map(|case| SkipReasonObservation {
                name: case.name,
                reason: case.failure.skip_reason(),
            })
            .collect::<Vec<_>>();
        let want = cases
            .iter()
            .map(|case| SkipReasonObservation {
                name: case.name,
                reason: case.expected,
            })
            .collect::<Vec<_>>();
        assert2::assert!(got == want);
    }

    /// A store error raised inside the Parquet reader still has to be
    /// reachable as an `object_store::Error`, which is what a caller matching
    /// `NotFound` needs and what a stringified error denies it.
    #[test]
    fn a_store_error_wrapped_by_the_parquet_reader_is_still_matchable() {
        let failure =
            BlockReadFailure::Parquet(ParquetError::External(Box::new(not_found("b1.parquet"))));
        assert2::assert!(let Some(ObjectStoreError::NotFound { .. }) = failure.object_store());
        assert2::assert!(failure.is_missing());
    }

    #[test]
    fn an_unreadable_block_reports_the_key_and_the_reason() {
        let error = BlockStoreError::block_unreadable(
            "blocks/b1.parquet",
            BlockReadFailure::ObjectStore(not_found("blocks/b1.parquet")),
        );
        assert2::assert!(
            error.skipped_block()
                == Some(SkippedBlock {
                    object_key: "blocks/b1.parquet".to_string(),
                    reason: BlockSkipReason::Missing,
                    detail: "Object at location blocks/b1.parquet not found: no such object"
                        .to_string(),
                })
        );
        assert2::assert!(error.is_block_missing());
    }

    /// An unreachable store produces no report entry, so a caller that skips
    /// whatever `skipped_block` hands it cannot silently drop a block whose
    /// contents nobody has established.
    #[test]
    fn a_store_failure_is_not_a_skippable_block() {
        let error = BlockStoreError::block_unreadable(
            "blocks/b1.parquet",
            BlockReadFailure::ObjectStore(unreachable_store()),
        );
        assert2::assert!(error.skipped_block() == None);
        assert2::assert!(!error.is_block_missing());
    }

    #[test]
    fn other_errors_are_not_about_one_block() {
        let error = BlockStoreError::ObjectStore("the store is down".to_string());
        assert2::assert!(error.unreadable_block().is_none());
        assert2::assert!(error.skipped_block() == None);
    }

    /// The rendered forms end up in a `warnings` entry of a Loki or Tempo
    /// response, so they have to name the block and say what happened.
    #[test]
    fn a_skipped_block_reads_as_a_warning() {
        let skipped = SkippedBlock {
            object_key: "blocks/b1.parquet".to_string(),
            reason: BlockSkipReason::Corrupt,
            detail: "corrupt footer".to_string(),
        };
        assert2::assert!(
            skipped.to_string() == "block `blocks/b1.parquet` skipped (corrupt): corrupt footer"
        );
    }
}

mod block_read_failure;
mod block_store_error;

pub use block_read_failure::BlockReadFailure;
pub use block_store_error::{BlockStoreError, UnreadableBlock};

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, BlockStoreError>;

/// Why a scan left one block out of the result it returned.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum BlockSkipReason {
    /// The index named a key the object store does not have.
    Missing,

    /// The object is there, and is not a readable Parquet block.
    Corrupt,
}

impl Display for BlockSkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let word = match self {
            Self::Missing => "missing",
            Self::Corrupt => "corrupt",
        };
        f.write_str(word)
    }
}

/// One block a scan could not read and left out of its result.
///
/// `detail` is the backend error rendered as text, unlike
/// [`BlockReadFailure`](BlockReadFailure), which keeps it typed. The
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
