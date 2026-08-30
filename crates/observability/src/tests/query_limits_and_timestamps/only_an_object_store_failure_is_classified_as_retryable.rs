use super::*;

/// The two error classifiers decide whether a compaction failure came from
/// the OBJECT STORE, which is the retryable kind -- a transient 503 should
/// be retried where a malformed block never will be. Misclassifying either
/// way is bad in its own direction: retrying a permanent failure spins,
/// and giving up on a transient one loses data.
#[test]
pub(crate) fn only_an_object_store_failure_is_classified_as_retryable() {
    use krabka_blockstore::LogBlockStoreError as BlockStoreError;

    let is_object_store = super::super::prelude::compaction_error_is_object_store;
    let object_store_error = || {
        BlockStoreError::ObjectStore(object_store::Error::NotFound {
            path: "block".to_string(),
            source: "gone".into(),
        })
    };

    // The one that is.
    check!(super::super::prelude::block_store_error_is_object_store(
        &object_store_error()
    ));
    check!(is_object_store(
        &super::super::prelude::CompactionError::BlockStore(object_store_error())
    ));

    // Every other block-store failure is not, including an I/O error,
    // which also arrives while talking to storage but is not the object
    // store reporting it.
    let others = || {
        vec![
            BlockStoreError::EmptyBlockScan,
            BlockStoreError::InvalidTimeRange {
                start_ns: 10,
                end_ns: 1,
            },
            BlockStoreError::InvalidManifestVersion {
                actual: 1,
                expected: 2,
            },
            BlockStoreError::Io(std::io::Error::from(std::io::ErrorKind::NotFound)),
        ]
    };
    for error in others() {
        check!(
            !super::super::prelude::block_store_error_is_object_store(&error),
            "{error}"
        );
    }
    for error in others() {
        check!(!is_object_store(
            &super::super::prelude::CompactionError::BlockStore(error)
        ));
    }

    // And every compaction failure that is not a block-store one at all.
    check!(!is_object_store(
        &super::super::prelude::CompactionError::EmptyWalBatch
    ));
    check!(!is_object_store(
        &super::super::prelude::CompactionError::AllRowsDeleted
    ));
    check!(!is_object_store(
        &super::super::prelude::CompactionError::MissingWalPosition { timestamp_ns: 1 }
    ));
    check!(!is_object_store(
        &super::super::prelude::CompactionError::MixedTenant {
            expected: "a".to_string(),
            actual: "b".to_string(),
        }
    ));
    check!(!is_object_store(
        &super::super::prelude::CompactionError::MixedPartition {
            expected: 1,
            actual: 2,
        }
    ));
}
