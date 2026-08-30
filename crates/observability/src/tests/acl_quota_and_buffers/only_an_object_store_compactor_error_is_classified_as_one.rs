use super::*;

/// Whether a compactor run failed on the object store decides whether the
/// run is retried, and every variant that is not one has to say so. With
/// the classifier stuck at true, a decode failure or a missing commit
/// position would be retried forever.
#[test]
pub(crate) fn only_an_object_store_compactor_error_is_classified_as_one() {
    use super::super::prelude::{CompactionFrontierStoreError, CompactorRunError};

    check!(super::super::prelude::compactor_run_error_is_object_store(
        &CompactorRunError::Frontier(CompactionFrontierStoreError::ObjectStore(
            object_store::Error::NotFound {
                path: "p".to_string(),
                source: "gone".into(),
            }
        ))
    ));
    check!(!super::super::prelude::compactor_run_error_is_object_store(
        &CompactorRunError::MissingCommitPosition
    ));
    check!(!super::super::prelude::compactor_run_error_is_object_store(
        &CompactorRunError::Frontier(CompactionFrontierStoreError::InvalidVersion {
            expected: 1,
            actual: 2,
        })
    ));
}
