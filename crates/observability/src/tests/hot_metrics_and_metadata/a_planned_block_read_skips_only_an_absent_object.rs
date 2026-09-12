use super::*;
use crate::read_planned_log_block;

/// `read_planned_log_block` skips one failure and one only: the block object
/// is not there. The retention sweep deletes block objects while queries run,
/// so a block the plan named can disappear between the plan and the read, and
/// the read answers `None` for it rather than failing the request.
///
/// The second half of this test is the half that matters later. A block
/// object that is PRESENT but is not a Parquet block is a real fault, and it
/// stays an error. Without that case pinned, the tolerance could widen to
/// swallow a decode bug, and every query would then return short results with
/// nothing to say so.
///
/// Both stores are checked, because each reports an absent block in its own
/// way: an object store answers `NotFound`, and the local filesystem answers
/// an I/O error.
#[tokio::test]
pub(crate) async fn a_planned_block_read_skips_only_an_absent_object() {
    use krabka_blockstore::{
        BlockKey, LogRow, TimeRange, log_block_object_path, write_log_block_to_object_store,
    };
    use object_store::memory::InMemory;

    use super::super::prelude::{Arc, ObjectPath, ObjectStore, ObjectStoreExt as _, QuerierState};

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = ObjectPath::from("indexes");
    let dir = tempfile::tempdir().expect("a temp dir");
    let range = TimeRange::new(0, 100).expect("a valid range");

    // One block that is there, one that never was, and one whose object is
    // present but holds bytes that are not a Parquet block.
    let present_key = BlockKey::new("tenant", 0, 0, 0, range);
    write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &present_key,
        vec![LogRow::new(1, 10, "line", BTreeMap::new())],
    )
    .await
    .expect("the block writes");

    let absent_key = BlockKey::new("tenant", 0, 1, 1, range);

    let corrupt_key = BlockKey::new("tenant", 0, 2, 2, range);
    store
        .put(
            &log_block_object_path(&prefix, &corrupt_key),
            b"not a parquet block".to_vec().into(),
        )
        .await
        .expect("the object writes");

    let cold = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default())
        .with_cold_object_store_source(Arc::clone(&store), prefix.clone());

    check!(
        read_planned_log_block(&cold, &present_key)
            .await
            .expect("a readable block is not an error")
            .is_some(),
        "the block that is there reads"
    );
    check!(
        read_planned_log_block(&cold, &absent_key)
            .await
            .expect("an absent block is not an error")
            .is_none(),
        "a swept block object degrades to a skip"
    );
    check!(
        read_planned_log_block(&cold, &corrupt_key).await.is_err(),
        "a block that is present but malformed is still a fault"
    );

    // The local store, whose absent block arrives as an I/O error rather
    // than as an object-store `NotFound`.
    let local = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default());
    check!(
        read_planned_log_block(&local, &absent_key)
            .await
            .expect("an absent local block is not an error")
            .is_none(),
        "a deleted local block file degrades to a skip too"
    );
}
