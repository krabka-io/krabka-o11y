    /// `append_matching_hot_metric_record` folds one uncompacted WAL record
    /// into the samples for every evaluation window it belongs to. The window
    /// is HALF-OPEN -- `(end - range, end]` -- which is `rate()`'s own
    /// semantics: a record exactly at a window's end is inside it, and one
    /// exactly at the start belongs to the previous window instead. Without
    /// that, a record on a boundary would be counted twice.
    ///
    /// Records are also skipped for the wrong tenant or when already
    /// compacted, so the hot tier does not double-count what the blocks
    /// already hold. Each of those is broken alone against a record the rest
    /// accepts.
    #[tokio::test]
    async fn a_hot_metric_record_lands_in_every_window_that_contains_it() {
        use krabka_logql::parse_metric_query;

        let query = parse_metric_query("count_over_time({app=\"api\"}[10s])")
            .expect("the metric query parses");
        let record = |tenant: &str, timestamp_ns| {
            let mut labels = Labels::default();
            labels.insert("app".to_string(), "api".to_string());
            super::WalLogRecord {
                tenant: tenant.to_string(),
                labels,
                timestamp_ns,
                line: "line".to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }
        };
        let plan = krabka_logql::StreamPlan {
            tenant: "tenant".to_string(),
            time_range: krabka_blockstore::TimeRange::new(0, 1_000_000_000_000)
                .expect("a valid range"),
            query: query.stream.clone(),
            fingerprints: BTreeSet::new(),
            blocks: Vec::new(),
        };
        let range_ns = 10_000_000_000_i64;
        // Two windows, ten seconds apart, so a record can land in one, both,
        // or neither.
        let eval_times = [20_000_000_000_i64, 30_000_000_000_i64];

        let windows_hit = |record: &super::WalLogRecord, frontier: &super::CompactionFrontier| {
            let mut samples = BTreeMap::new();
            super::append_matching_hot_metric_record(
                &mut samples,
                &plan,
                record,
                frontier,
                super::MetricWindow {
                    query: &query,
                    eval_times: &eval_times,
                    range_ns,
                    delete_filters: &[],
                },
            )
            .expect("the record folds in");
            samples
                .values()
                .flat_map(BTreeMap::keys)
                .copied()
                .collect::<BTreeSet<_>>()
        };
        let open = super::CompactionFrontier::new(0);

        // Exactly at a window's end: inside that window.
        check!(
            windows_hit(&record("tenant", 20_000_000_000), &open) == [20_000_000_000].into(),
            "a record at the window end is inside it"
        );
        // Exactly at a window's start: NOT in it -- it belongs to the window
        // before, which is not being evaluated here.
        check!(
            windows_hit(&record("tenant", 10_000_000_000), &open).is_empty(),
            "a record at the window start belongs to the previous window"
        );
        // One nanosecond past the start is inside.
        check!(windows_hit(&record("tenant", 10_000_000_001), &open) == [20_000_000_000].into());
        // In the overlap of neither window.
        check!(windows_hit(&record("tenant", 5_000_000_000), &open).is_empty());
        // Inside the second window only.
        check!(windows_hit(&record("tenant", 25_000_000_000), &open) == [30_000_000_000].into());

        // A record for another tenant is skipped even when it is in range.
        check!(windows_hit(&record("other", 20_000_000_000), &open).is_empty());

        // A record the blocks already hold is skipped, so the hot tier does
        // not double-count it.
        let compacted = super::CompactionFrontier::new(21_000_000_000);
        check!(
            windows_hit(&record("tenant", 20_000_000_000), &compacted).is_empty(),
            "already compacted"
        );

        // An `offset` shifts the window BACK in time. Without one the offset
        // is zero and adding it reads the same as subtracting, so this needs
        // its own query: offset 5s puts the window for eval time 20s at
        // (5s, 15s], where a record at exactly 15s is inside. Added instead,
        // the window would be (15s, 25s] and 15s would fall outside it.
        let offset_query = parse_metric_query("count_over_time({app=\"api\"}[10s] offset 5s)")
            .expect("the offset query parses");
        let mut samples = BTreeMap::new();
        super::append_matching_hot_metric_record(
            &mut samples,
            &plan,
            &record("tenant", 15_000_000_000),
            &open,
            super::MetricWindow {
                query: &offset_query,
                eval_times: &[20_000_000_000],
                range_ns,
                delete_filters: &[],
            },
        )
        .expect("the record folds in");
        // The INNER keys, not whether `samples` has anything in it: the outer
        // entry for the series is created as soon as the record matches the
        // query, before any window is considered, so an empty-map check would
        // pass whatever the windows decided.
        check!(
            samples
                .values()
                .flat_map(BTreeMap::keys)
                .copied()
                .collect::<BTreeSet<_>>()
                == [20_000_000_000].into(),
            "the offset moves the window back, not forward"
        );
    }

    /// `parse_label_replace_metric_binary_expression` recognises a binary
    /// expression where EITHER side is a `label_replace(...)`, and reports
    /// which kind of binary it is. The three kinds are tried in order --
    /// arithmetic, comparison, set -- and each must produce its own variant,
    /// since they are handled by different evaluators downstream.
    ///
    /// Either side qualifying is the point: a `label_replace` on the right
    /// alone is just as much this shape as one on the left, and the two go
    /// through the same `||`.
    #[test]
    fn a_label_replace_binary_expression_names_its_own_kind() {
        use super::LabelReplaceMetricBinaryExpression as Expression;

        let parse = super::parse_label_replace_metric_binary_expression;
        let replace = r#"label_replace(up,"a","b","c","d")"#;

        // Arithmetic, with the label_replace on each side in turn.
        check!(matches!(
            parse(&format!("{replace} + up")),
            Some(Expression::Arithmetic { .. })
        ));
        check!(
            matches!(
                parse(&format!("up + {replace}")),
                Some(Expression::Arithmetic { .. })
            ),
            "on the right is equally this shape"
        );

        // Comparison and set each get their own variant.
        check!(matches!(
            parse(&format!("{replace} > up")),
            Some(Expression::Comparison { .. })
        ));
        check!(matches!(
            parse(&format!("{replace} and up")),
            Some(Expression::Set { .. })
        ));

        // The operands are carried through trimmed, not with the whitespace
        // the split left on them.
        let Some(Expression::Arithmetic { left, right, .. }) = parse(&format!("{replace}  +  up"))
        else {
            panic!("an arithmetic expression");
        };
        check!(left == replace, "the left operand is trimmed");
        check!(right == "up", "and so is the right");

        // The operator is carried through, not assumed. Subtraction is used
        // because it is not the variant a collapsed arm would default to.
        let Some(Expression::Arithmetic { op, .. }) = parse(&format!("{replace} - up")) else {
            panic!("an arithmetic expression");
        };
        check!(op == krabka_logql::MetricScalarArithmeticOp::Subtract);
        let Some(Expression::Comparison { op, .. }) = parse(&format!("{replace} < up")) else {
            panic!("a comparison expression");
        };
        check!(op == krabka_logql::ComparisonOp::Less);

        // A binary expression with no label_replace on either side is not this
        // shape, and is parsed elsewhere.
        check!(parse("up + down").is_none());
        check!(parse("up > down").is_none());
        check!(parse("up and down").is_none());

        // Nor is a bare label_replace with no binary operator at all.
        check!(parse(replace).is_none());
        check!(parse("").is_none());
    }

    /// `sample_time_bucket` floors a sample onto the step grid measured FROM
    /// the query's start, not from the epoch. A start that is not itself a
    /// multiple of the step is what shows that: with start 0 the two are the
    /// same arithmetic, and every bucket would look right.
    ///
    /// A sample before the start clamps to the start rather than producing a
    /// bucket before the window began. The `<=` in that guard could be `<`
    /// without changing any answer -- at exactly the start the arithmetic
    /// yields the start anyway -- so relaxing it is an equivalent mutation.
    /// The guard as a whole is not: a sample below the start would otherwise
    /// floor to a negative offset.
    #[test]
    fn a_sample_buckets_onto_the_grid_measured_from_the_query_start() {
        let bucket = super::sample_time_bucket;
        // 1_000 is deliberately not a multiple of 300.
        let (start, step) = (1_000_i64, 300_i64);

        // The grid runs 1000, 1300, 1600 -- not 900, 1200, 1500, which is what
        // flooring from the epoch would give.
        check!(
            bucket(1_000, start, step) == 1_000,
            "the start is its own bucket"
        );
        check!(bucket(1_001, start, step) == 1_000);
        check!(bucket(1_299, start, step) == 1_000, "one short of the next");
        check!(bucket(1_300, start, step) == 1_300, "exactly on the next");
        check!(bucket(1_301, start, step) == 1_300);
        check!(bucket(1_900, start, step) == 1_900, "three steps along");
        check!(bucket(2_000, start, step) == 1_900);

        // At or before the start, clamped.
        check!(bucket(999, start, step) == 1_000);
        check!(bucket(0, start, step) == 1_000);
        check!(bucket(-1_000, start, step) == 1_000);

        // A start of zero is the degenerate case where flooring from the start
        // and from the epoch agree -- pinned so the distinction above is not
        // mistaken for the only behaviour.
        check!(bucket(700, 0, step) == 600);
        check!(bucket(600, 0, step) == 600);
    }

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
    async fn missing_metadata_blocks_fall_back_to_their_indexed_fingerprints() {
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
        let missing = krabka_blockstore::BlockDescriptor::new(
            missing_key,
            [7_u64, 8_u64].into_iter().collect(),
        );

        let mut index = BlockIndex::default();
        index.insert(present);
        index.insert(missing);
        let state = super::QuerierState::new(dir.path(), LabelIndex::default(), index);

        let series = |time_range| {
            let state = &state;
            async move {
                super::metadata_fingerprints_in_time_range(state, "tenant", time_range)
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
    async fn counting_index_stats_reads_only_the_rows_a_plan_would() {
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

        let state =
            super::QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default());
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
                super::count_index_stats_entries(state, &plan)
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

