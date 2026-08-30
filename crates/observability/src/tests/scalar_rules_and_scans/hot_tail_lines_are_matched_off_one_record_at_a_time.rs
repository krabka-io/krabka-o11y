use super::*;

/// `count_loki_stream_result_hot_tail_lines` reports how many lines of a
/// response came from the hot tail rather than from blocks. It is a
/// MULTISET match-off: each hot record can account for one response line
/// and no more, so two identical records admit two identical lines and a
/// third line is attributed to the blocks.
///
/// That counting-down is the whole design, and one record with one line
/// cannot show it -- a plain membership test would agree.
///
/// The key is built from the QUERY's output labels rather than the
/// record's raw ones, which is why the response streams here carry a
/// `detected_level` the records do not: the query synthesises it, so a
/// response without it matches nothing at all.
#[test]
pub(crate) fn hot_tail_lines_are_matched_off_one_record_at_a_time() {
    use krabka_logql::StreamPlan;

    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: krabka_blockstore::TimeRange::new(0, 100).expect("a valid range"),
        query: krabka_logql::parse_query("{app=\"api\"}").expect("the query parses"),
        fingerprints: BTreeSet::new(),
        blocks: Vec::new(),
    };
    let record = |tenant: &str, timestamp_ns, line: &str| super::super::prelude::WalLogRecord {
        tenant: tenant.to_string(),
        labels: labels.clone(),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    let response = |entries: &[(i64, &str)]| {
        serde_json::json!({
            "data": {"result": [{
                "stream": {"app": "api", "detected_level": "unknown"},
                "values": entries
                    .iter()
                    .map(|(ts, line)| serde_json::json!([ts.to_string(), line]))
                    .collect::<Vec<_>>(),
            }]}
        })
    };
    let open = super::super::prelude::CompactionFrontier::new(0);
    let counted = |value: &serde_json::Value, hot: &[super::super::prelude::WalLogRecord]| {
        super::super::prelude::count_loki_stream_result_hot_tail_lines(value, &plan, hot, &open)
    };

    // One record, one matching line.
    check!(counted(&response(&[(10, "a")]), &[record("tenant", 10, "a")]) == 1);

    // A line the hot tail does not hold came from the blocks.
    check!(counted(&response(&[(10, "b")]), &[record("tenant", 10, "a")]) == 0);
    check!(counted(&response(&[(20, "a")]), &[record("tenant", 10, "a")]) == 0);

    // The multiset: two identical records admit two identical lines, and
    // a third is attributed to the blocks rather than counted again.
    let twice = [record("tenant", 10, "a"), record("tenant", 10, "a")];
    check!(counted(&response(&[(10, "a")]), &twice) == 1);
    check!(counted(&response(&[(10, "a"), (10, "a")]), &twice) == 2);
    check!(
        counted(&response(&[(10, "a"), (10, "a"), (10, "a")]), &twice) == 2,
        "two records cannot account for three lines"
    );

    // Hot records are filtered before the match-off: another tenant, a
    // timestamp outside the plan's range, and one already compacted.
    check!(counted(&response(&[(10, "a")]), &[record("other", 10, "a")]) == 0);
    // Both ends of the range, since they are separate clauses. Past the
    // end is straightforward. BEFORE the start needs its own plan: with
    // this plan starting at zero, anything earlier is also behind the
    // compaction frontier and would be filtered by that instead, leaving
    // the start-of-range clause untested.
    check!(counted(&response(&[(200, "a")]), &[record("tenant", 200, "a")]) == 0);
    let later_plan = StreamPlan {
        time_range: krabka_blockstore::TimeRange::new(50, 100).expect("a valid range"),
        ..plan.clone()
    };
    check!(
        super::super::prelude::count_loki_stream_result_hot_tail_lines(
            &response(&[(10, "a")]),
            &later_plan,
            &[record("tenant", 10, "a")],
            &open,
        ) == 0,
        "before the range start, but after the frontier"
    );
    let compacted = super::super::prelude::CompactionFrontier::new(50);
    check!(
        super::super::prelude::count_loki_stream_result_hot_tail_lines(
            &response(&[(10, "a")]),
            &plan,
            &[record("tenant", 10, "a")],
            &compacted,
        ) == 0,
        "a compacted record is the blocks' line, not the hot tail's"
    );

    // Nothing on either side.
    check!(counted(&response(&[]), &[record("tenant", 10, "a")]) == 0);
    check!(counted(&response(&[(10, "a")]), &[]) == 0);
}
