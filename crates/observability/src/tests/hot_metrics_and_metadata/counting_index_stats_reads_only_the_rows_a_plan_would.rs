use super::*;

/// `count_index_stats_entries` counts the rows a plan would actually read:
/// those whose series is in the plan AND whose timestamp falls inside its
/// range. All three conditions are and-ed, so each is broken alone against
/// a row the other two accept.
///
/// Both bounds are INCLUSIVE here, unlike `count_stream_map_lines` whose
/// end is exclusive. The two count different things -- one the rows on
/// disk, the other the lines already returned -- so the difference is
/// deliberate, and each is pinned at its own boundary.
#[tokio::test]
pub(crate) async fn counting_index_stats_reads_only_the_rows_a_plan_would() {
    use krabka_blockstore::{BlockKey, LogRow, TimeRange, write_log_block};
    use krabka_logql::{StreamPlan, StreamQuery};

    let dir = tempfile::tempdir().expect("a temp dir");
    let key = BlockKey::new(
        "tenant",
        0,
        0,
        0,
        TimeRange::new(0, 100).expect("a valid range"),
    );
    let row = |fingerprint: u64, timestamp_ns| LogRow {
        series_fingerprint: fingerprint,
        timestamp_ns,
        line: "line".to_string(),
        structured_metadata: BTreeMap::new(),
    };
    // Two series, and timestamps sitting on and either side of the bounds
    // the plan will use.
    let descriptor = write_log_block(
        dir.path(),
        &key,
        vec![
            row(1, 9),
            row(1, 10),
            row(1, 50),
            row(1, 90),
            row(1, 91),
            row(2, 50),
        ],
    )
    .expect("the block writes");

    let state = super::super::prelude::QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let plan = |fingerprints: &[u64], start_ns, end_ns| StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(start_ns, end_ns).expect("a valid range"),
        query: StreamQuery {
            matchers: Vec::new(),
            pipeline: Vec::new(),
        },
        fingerprints: fingerprints.iter().copied().collect(),
        blocks: vec![descriptor.clone()],
    };
    let count = |plan: StreamPlan| {
        let state = &state;
        async move {
            super::super::prelude::count_index_stats_entries(state, &plan)
                .await
                .expect("the block reads")
        }
    };

    // Series 1 has rows at 9, 10, 50, 90 and 91. Within 10..=90 that is
    // three: the ones at 9 and 91 fall outside, and series 2's row is a
    // different series.
    check!(
        count(plan(&[1], 10, 90)).await == 3,
        "both bounds inclusive"
    );

    // Each bound moved in by one drops the row sitting exactly on it,
    // which is what makes the bounds observably inclusive.
    check!(
        count(plan(&[1], 11, 90)).await == 2,
        "the row at 10 is dropped"
    );
    check!(count(plan(&[1], 10, 89)).await == 2, "and the row at 90");

    // The series filter, alone.
    check!(count(plan(&[2], 0, 100)).await == 1, "only series 2's row");
    check!(
        count(plan(&[1, 2], 0, 100)).await == 6,
        "both series, whole range"
    );
    check!(count(plan(&[], 0, 100)).await == 0, "no series, no rows");

    // A range that excludes everything, and a plan with no blocks.
    check!(count(plan(&[1, 2], 200, 300)).await == 0);
    let mut empty = plan(&[1], 0, 100);
    empty.blocks.clear();
    check!(count(empty).await == 0, "no blocks, nothing to read");

    // Two blocks are SUMMED. With one block, accumulating and replacing
    // give the same answer, so a second block is what makes the running
    // total observable.
    let second_key = BlockKey::new(
        "tenant",
        0,
        1,
        1,
        TimeRange::new(0, 100).expect("a valid range"),
    );
    let second = write_log_block(dir.path(), &second_key, vec![row(1, 20), row(1, 30)])
        .expect("the second block writes");
    let mut both = plan(&[1], 0, 100);
    both.blocks.push(second);
    check!(
        count(both).await == 7,
        "five rows in the first block and two in the second"
    );
}
