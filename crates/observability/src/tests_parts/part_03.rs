    /// `metric_scalar_comparison_matches` compares a sample against a scalar,
    /// with a flag saying which side the scalar was written on. That flag only
    /// matters for the four ordered operators -- `1 > x` and `x > 1` disagree
    /// where `1 == x` and `x == 1` do not -- so every operator is checked at
    /// all three orderings AND on both sides.
    ///
    /// The two regex operators are always false here: a regex against a number
    /// is not a comparison `LogQL` can evaluate, and answering either way would
    /// silently filter samples on a predicate nobody wrote.
    #[test]
    fn a_scalar_comparison_answers_every_operator_from_both_sides() {
        use std::cmp::Ordering;

        use krabka_logql::ComparisonOp;

        let one = MetricValue::new(1, 1);
        let two = MetricValue::new(2, 1);
        let matches = |sample, op, scalar, scalar_on_left| {
            super::metric_scalar_comparison_matches(sample, op, scalar, scalar_on_left)
        };

        // (ordering of left against right, sample, scalar, scalar_on_left)
        let cases = [
            (Ordering::Less, one, two, false),
            (Ordering::Greater, one, two, true),
            (Ordering::Greater, two, one, false),
            (Ordering::Less, two, one, true),
            (Ordering::Equal, one, one, false),
            (Ordering::Equal, one, one, true),
        ];
        for (ordering, sample, scalar, scalar_on_left) in cases {
            let want = |op| match op {
                ComparisonOp::Equal => ordering == Ordering::Equal,
                ComparisonOp::NotEqual => ordering != Ordering::Equal,
                ComparisonOp::Greater => ordering == Ordering::Greater,
                ComparisonOp::GreaterEqual => ordering != Ordering::Less,
                ComparisonOp::Less => ordering == Ordering::Less,
                ComparisonOp::LessEqual => ordering != Ordering::Greater,
                ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
            };
            for op in [
                ComparisonOp::Equal,
                ComparisonOp::NotEqual,
                ComparisonOp::Greater,
                ComparisonOp::GreaterEqual,
                ComparisonOp::Less,
                ComparisonOp::LessEqual,
                ComparisonOp::RegexEqual,
                ComparisonOp::RegexNotEqual,
            ] {
                check!(
                    matches(sample, op, scalar, scalar_on_left) == want(op),
                    "{op:?} at {ordering:?} with scalar_on_left={scalar_on_left}"
                );
            }
        }

        // Spelled out for the case the table exists to protect: the scalar's
        // side changes the answer for an ordered operator and not for equality.
        check!(
            matches(one, ComparisonOp::Less, two, false),
            "x < 1 where x is smaller"
        );
        check!(
            !matches(one, ComparisonOp::Less, two, true),
            "but 1 < x is not"
        );
        check!(matches(one, ComparisonOp::Equal, one, false));
        check!(
            matches(one, ComparisonOp::Equal, one, true),
            "equality is side-blind"
        );
    }

    /// `page_groups` pages the rules response by group, resuming AFTER the
    /// token the client sent rather than at it -- resuming at it would return
    /// the same group forever. The token it hands back names the LAST group in
    /// the page, which is what makes the next request resume correctly.
    ///
    /// The two are checked against each other by walking a five-group list to
    /// exhaustion in pages of two: an off-by-one in either the resume or the
    /// token shows up as a repeated or skipped group rather than as a wrong
    /// count.
    #[test]
    fn paging_rule_groups_resumes_after_the_token_it_handed_back() {
        let groups = || {
            ["a", "b", "c", "d", "e"]
                .iter()
                .map(|name| super::PrometheusRuleGroupResponse {
                    token: (*name).to_string(),
                    value: serde_json::json!({"name": name}),
                })
                .collect::<Vec<_>>()
        };
        let page = |limit: Option<usize>, token: Option<&str>| {
            super::PrometheusRulesFilters {
                group_limit: limit,
                group_next_token: token.map(str::to_string),
                ..super::PrometheusRulesFilters::default()
            }
            .page_groups(groups())
        };
        let names = |page: &super::PrometheusRulesPage| {
            page.groups
                .iter()
                .map(|group| group["name"].as_str().expect("a name").to_string())
                .collect::<Vec<_>>()
        };

        // No limit returns everything, with nothing to resume from.
        let all = page(None, None).expect("no limit is valid");
        check!(names(&all) == vec!["a", "b", "c", "d", "e"]);
        check!(all.next_token.is_none());

        // Walk the list in pages of two. The token names the last group
        // returned, and the next page starts after it.
        let first = page(Some(2), None).expect("a first page");
        check!(names(&first) == vec!["a", "b"]);
        check!(
            first.next_token.as_deref() == Some("b"),
            "the LAST group of the page"
        );

        let second = page(Some(2), Some("b")).expect("a second page");
        check!(
            names(&second) == vec!["c", "d"],
            "resumes after b, not at it"
        );
        check!(second.next_token.as_deref() == Some("d"));

        // The final page is short and offers no token, because nothing follows.
        let third = page(Some(2), Some("d")).expect("a third page");
        check!(names(&third) == vec!["e"]);
        check!(third.next_token.is_none());

        // A page that exactly exhausts the list offers no token either: the
        // boundary is `>` and not `>=`, or a client would ask for an empty page.
        let exact = page(Some(5), None).expect("an exact page");
        check!(names(&exact) == vec!["a", "b", "c", "d", "e"]);
        check!(exact.next_token.is_none(), "nothing follows an exact fit");

        // A zero limit returns nothing and offers no token to resume from,
        // rather than a token that would never advance.
        let none = page(Some(0), None).expect("a zero limit is valid");
        check!(names(&none).is_empty());
        check!(none.next_token.is_none());

        // Resuming from the last group leaves an empty page.
        let past = page(Some(2), Some("e")).expect("resuming from the end");
        check!(names(&past).is_empty());

        // A token naming no group is a client error, not an empty page: it
        // usually means the group was deleted between requests.
        check!(page(Some(2), Some("nonsense")).is_err());
    }

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
    fn hot_tail_lines_are_matched_off_one_record_at_a_time() {
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
        let record = |tenant: &str, timestamp_ns, line: &str| super::WalLogRecord {
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
        let open = super::CompactionFrontier::new(0);
        let counted = |value: &serde_json::Value, hot: &[super::WalLogRecord]| {
            super::count_loki_stream_result_hot_tail_lines(value, &plan, hot, &open)
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
            super::count_loki_stream_result_hot_tail_lines(
                &response(&[(10, "a")]),
                &later_plan,
                &[record("tenant", 10, "a")],
                &open,
            ) == 0,
            "before the range start, but after the frontier"
        );
        let compacted = super::CompactionFrontier::new(50);
        check!(
            super::count_loki_stream_result_hot_tail_lines(
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

    /// `append_matching_log_row` decides whether one row belongs in the
    /// response. Its first guard is three conditions or-ed as REJECTIONS --
    /// too early, too late, or not a series the plan wants -- so each rejects
    /// on its own against a row the other two accept.
    ///
    /// Both range bounds are inclusive, pinned by rows sitting exactly on
    /// each. A row whose series the label index cannot name is an ERROR rather
    /// than a skip: the plan asked for that series, so being unable to label
    /// it means the index disagrees with the plan.
    #[test]
    fn a_log_row_is_appended_only_when_the_plan_asked_for_it() {
        use krabka_logql::StreamPlan;

        let mut label_index = LabelIndex::default();
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        let known = label_index.insert_series("tenant", labels);
        let mut other = Labels::default();
        other.insert("app".to_string(), "web".to_string());
        let unwanted = label_index.insert_series("tenant", other);

        let plan = StreamPlan {
            tenant: "tenant".to_string(),
            time_range: krabka_blockstore::TimeRange::new(10, 90).expect("a valid range"),
            query: krabka_logql::parse_query("{app=\"api\"}").expect("the query parses"),
            fingerprints: [known].into_iter().collect(),
            blocks: Vec::new(),
        };
        let metadata = Labels::default();
        let appended = |fingerprint, timestamp_ns| {
            let mut streams = BTreeMap::new();
            let result = super::append_matching_log_row(
                &mut streams,
                &plan,
                &label_index,
                super::QueryRow {
                    fingerprint,
                    timestamp_ns,
                    line: "line",
                    structured_metadata: &metadata,
                },
                &[],
            );
            result.map(|()| streams.values().map(Vec::len).sum::<usize>())
        };

        // Inside the range, and a series the plan wants.
        check!(appended(known, 50).ok() == Some(1));
        // Exactly on each bound: both inclusive.
        check!(
            appended(known, 10).ok() == Some(1),
            "the start bound is inclusive"
        );
        check!(appended(known, 90).ok() == Some(1), "and so is the end");
        // One step outside each.
        check!(appended(known, 9).ok() == Some(0), "before the range");
        check!(appended(known, 91).ok() == Some(0), "after it");
        // A series the plan did not ask for, inside the range.
        check!(
            appended(unwanted, 50).ok() == Some(0),
            "not a wanted series"
        );

        // A fingerprint the label index cannot name is an error, not a skip --
        // but only once the row has passed the range and series filters, so a
        // nameless series the plan never wanted is still simply skipped.
        let nameless = 999_999_u64;
        check!(
            appended(nameless, 50).ok() == Some(0),
            "not wanted, so not named"
        );
        let mut wants_nameless = plan.clone();
        wants_nameless.fingerprints.insert(nameless);
        let mut streams = BTreeMap::new();
        check!(matches!(
            super::append_matching_log_row(
                &mut streams,
                &wants_nameless,
                &label_index,
                super::QueryRow {
                    fingerprint: nameless,
                    timestamp_ns: 50,
                    line: "line",
                    structured_metadata: &metadata,
                },
                &[],
            ),
            Err(super::QueryError::MissingSeriesLabels { .. })
        ));
    }

