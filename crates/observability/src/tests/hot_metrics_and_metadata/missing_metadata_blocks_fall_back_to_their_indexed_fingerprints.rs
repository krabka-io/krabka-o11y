use super::*;

/// `metadata_fingerprints_in_time_range` collects the series present in a
/// window, and does something worth pinning when a block's FILE is gone:
/// it falls back to the fingerprints the index already records for that
/// block, rather than failing the whole request. The index knows which
/// series a block held, so a deleted or not-yet-fetched file degrades to a
/// coarser answer instead of no answer.
///
/// The fallback is coarser in a specific way: it ignores the time range,
/// since without the rows there is nothing to filter. The test shows that
/// by asking for a window the missing block's rows would have fallen
/// outside of, and still getting its series back.
#[tokio::test]
pub(crate) async fn missing_metadata_blocks_fall_back_to_their_indexed_fingerprints() {
    use krabka_blockstore::{BlockKey, LogRow, TimeRange, write_log_block};

    let dir = tempfile::tempdir().expect("a temp dir");
    let range = |start_ns, end_ns| TimeRange::new(start_ns, end_ns).expect("a valid range");
    let row = |fingerprint: u64, timestamp_ns| LogRow {
        series_fingerprint: fingerprint,
        timestamp_ns,
        line: "line".to_string(),
        structured_metadata: BTreeMap::new(),
    };

    // One block that exists on disk, holding two series at 10 and 90.
    let present_key = BlockKey::new("tenant", 0, 0, 0, range(0, 100));
    let present = write_log_block(dir.path(), &present_key, vec![row(1, 10), row(2, 90)])
        .expect("the block writes");

    // One block the index knows about whose file was never written.
    let missing_key = BlockKey::new("tenant", 0, 1, 1, range(0, 100));
    let missing =
        krabka_blockstore::BlockDescriptor::new(missing_key, [7_u64, 8_u64].into_iter().collect());

    let mut index = BlockIndex::default();
    index.insert(present);
    index.insert(missing);
    let state = super::super::prelude::QuerierState::new(dir.path(), LabelIndex::default(), index);

    let series = |time_range| {
        let state = &state;
        async move {
            super::super::prelude::metadata_fingerprints_in_time_range(state, "tenant", time_range)
                .await
                .expect("the metadata reads")
        }
    };

    // The whole window: both real series, plus the missing block's two.
    check!(
        series(range(0, 100)).await == [1_u64, 2, 7, 8].into_iter().collect(),
        "the indexed fingerprints stand in for the unreadable block"
    );

    // A narrow window excludes the row at 90 from the block that EXISTS,
    // but the missing block still contributes both of its series -- the
    // fallback cannot filter by time.
    check!(
        series(range(0, 50)).await == [1_u64, 7, 8].into_iter().collect(),
        "the fallback ignores the range it cannot check"
    );

    // A window ending exactly on a row keeps it: both bounds are
    // inclusive, and no other range here puts a row on its edge.
    check!(
        series(range(0, 90)).await == [1_u64, 2, 7, 8].into_iter().collect(),
        "the row at 90 is inside a window ending at 90"
    );
    check!(
        series(range(10, 89)).await == [1_u64, 7, 8].into_iter().collect(),
        "and outside one ending at 89"
    );

    // A window matching no block at all yields nothing.
    check!(series(range(1_000, 2_000)).await.is_empty());
}
