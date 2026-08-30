use super::*;

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
pub(crate) async fn a_hot_metric_record_lands_in_every_window_that_contains_it() {
    use krabka_logql::parse_metric_query;

    let query =
        parse_metric_query("count_over_time({app=\"api\"}[10s])").expect("the metric query parses");
    let record = |tenant: &str, timestamp_ns| {
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        super::super::prelude::WalLogRecord {
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
        time_range: krabka_blockstore::TimeRange::new(0, 1_000_000_000_000).expect("a valid range"),
        query: query.stream.clone(),
        fingerprints: BTreeSet::new(),
        blocks: Vec::new(),
    };
    let range_ns = 10_000_000_000_i64;
    // Two windows, ten seconds apart, so a record can land in one, both,
    // or neither.
    let eval_times = [20_000_000_000_i64, 30_000_000_000_i64];

    let windows_hit = |record: &super::super::prelude::WalLogRecord,
                       frontier: &super::super::prelude::CompactionFrontier| {
        let mut samples = BTreeMap::new();
        super::super::prelude::append_matching_hot_metric_record(
            &mut samples,
            &plan,
            record,
            frontier,
            super::super::prelude::MetricWindow {
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
    let open = super::super::prelude::CompactionFrontier::new(0);

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
    let compacted = super::super::prelude::CompactionFrontier::new(21_000_000_000);
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
    super::super::prelude::append_matching_hot_metric_record(
        &mut samples,
        &plan,
        &record("tenant", 15_000_000_000),
        &open,
        super::super::prelude::MetricWindow {
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
