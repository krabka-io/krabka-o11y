//! Error type for the block store.

use std::fmt::Display;

use object_store::Error as ObjectStoreError;
use parquet::errors::ParquetError;

#[cfg(test)]
mod tests {
    use super::*;

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
        let cases: Vec<(&str, BlockReadFailure, Option<BlockSkipReason>)> = vec![
            (
                "the object is gone",
                BlockReadFailure::ObjectStore(not_found("b1.parquet")),
                Some(BlockSkipReason::Missing),
            ),
            (
                "the object went while the footer was being read",
                BlockReadFailure::Parquet(ParquetError::External(Box::new(not_found(
                    "b1.parquet",
                )))),
                Some(BlockSkipReason::Missing),
            ),
            (
                "the bytes are not a parquet block",
                BlockReadFailure::Parquet(ParquetError::General("corrupt footer".to_string())),
                Some(BlockSkipReason::Corrupt),
            ),
            (
                "the store is unreachable",
                BlockReadFailure::ObjectStore(unreachable_store()),
                None,
            ),
            (
                "the store failed while the footer was being read",
                BlockReadFailure::Parquet(ParquetError::External(Box::new(unreachable_store()))),
                None,
            ),
        ];

        let got = cases
            .iter()
            .map(|(name, failure, _)| (*name, failure.skip_reason()))
            .collect::<Vec<_>>();
        let want = cases
            .iter()
            .map(|(name, _, reason)| (*name, *reason))
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
mod block_skip_reason;
mod block_store_error;
mod result;
mod skipped_block;

pub use block_read_failure::BlockReadFailure;
pub use block_skip_reason::BlockSkipReason;
pub use block_store_error::BlockStoreError;
pub use result::Result;
pub use skipped_block::SkippedBlock;
