    /// `format_loki_duration_ns` composes a duration from the largest unit
    /// down, SKIPPING units that contribute nothing -- so 3661s is "1h1m1s"
    /// and not "1h1m1s0ms0us0ns". Zero is the one duration spelled with a unit
    /// it does not contain, because "" would not read as a duration at all.
    #[test]
    fn a_loki_duration_composes_only_the_units_it_needs() {
        let format = super::format_loki_duration_ns;

        // Each unit alone.
        check!(format(3_600_000_000_000) == Some("1h".to_string()));
        check!(format(60_000_000_000) == Some("1m".to_string()));
        check!(format(1_000_000_000) == Some("1s".to_string()));
        check!(format(1_000_000) == Some("1ms".to_string()));
        check!(format(1_000) == Some("1us".to_string()));
        check!(format(1) == Some("1ns".to_string()));

        // Composed, with the gaps left out rather than written as zeros.
        check!(format(3_661_000_000_000) == Some("1h1m1s".to_string()));
        check!(
            format(3_600_000_000_001) == Some("1h1ns".to_string()),
            "no zero units between"
        );
        check!(format(90_000_000_000) == Some("1m30s".to_string()));
        check!(format(1_500_000) == Some("1ms500us".to_string()));

        // Counts above one, and a unit that repeats rather than rolling over
        // into the next -- 90 minutes is an hour and a half, not "90m".
        check!(format(2 * 3_600_000_000_000) == Some("2h".to_string()));
        check!(format(90 * 60_000_000_000) == Some("1h30m".to_string()));

        // Zero and negative are different answers: a zero duration is a
        // duration, a negative one is not.
        check!(format(0) == Some("0s".to_string()));
        check!(format(-1).is_none());
        check!(format(-3_600_000_000_000).is_none());
    }

    /// `is_bytes_literal` accepts "1MB" and "1.5GiB": a non-negative finite
    /// number followed by a unit it knows. The split is at the first letter,
    /// so the number and the unit are never ambiguous -- and both the decimal
    /// and binary spellings of each magnitude are units, since Loki accepts
    /// both.
    #[test]
    fn a_bytes_literal_needs_a_number_and_a_unit_it_knows() {
        let is_bytes = super::is_bytes_literal;

        for unit in [
            "B", "kB", "KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB",
        ] {
            check!(is_bytes(&format!("1{unit}")), "{unit}");
        }
        check!(is_bytes("1.5GiB"), "a fractional amount");
        check!(is_bytes("0B"), "zero bytes is a size");

        // A number with no unit, or a unit with no number.
        check!(!is_bytes("1"));
        check!(!is_bytes(""));
        check!(!is_bytes("MB"), "the amount is empty, which does not parse");

        // Units it does not know, including near-misses.
        check!(!is_bytes("1PB"));
        check!(!is_bytes("1mb"), "the units are case-sensitive");
        check!(!is_bytes("1MBs"));
        check!(!is_bytes("1Mib"));

        // A negative amount is refused rather than clamped to zero.
        check!(!is_bytes("-1MB"));

        // "inf" and "NaN" contain letters, so the split puts them in the UNIT
        // and leaves the amount empty -- they are refused for having no
        // number, not for being non-finite.
        check!(!is_bytes("infMB"));
        check!(!is_bytes("NaNMB"));

        // The finiteness check is reached by a number with no letters in it at
        // all: four hundred digits overflow an f64 to infinity, and a size of
        // infinity is not a size.
        let overflowing = format!("{}MB", "1".repeat(400));
        check!(
            !is_bytes(&overflowing),
            "an amount that overflows to infinity"
        );
    }

    /// `eligible_tail_record_count` holds a tail back by `delay_for`, so a
    /// consumer sees only records old enough that nothing earlier can still
    /// arrive. It counts with `take_while` rather than `filter`: the WAL is
    /// ordered, so the first record too new to send BLOCKS the ones after it
    /// even if those happen to be older. Sending past it would emit records
    /// out of order, which is worse than sending them late.
    #[test]
    fn a_tail_holds_back_records_newer_than_its_delay() {
        let record = |timestamp_ns| super::WalLogRecord {
            tenant: "tenant".to_string(),
            labels: Labels::default(),
            timestamp_ns,
            line: "line".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        };
        let count = super::eligible_tail_record_count;
        // Comfortably either side of now, so the wall clock cannot straddle
        // them however long the test takes to reach this line.
        let old = 1_000_000_000_000_i64;
        let future = i64::MAX / 2;

        // No delay means no holding back, whatever the timestamps.
        check!(count(&[record(old), record(future)], 0) == 2);
        check!(
            count(&[record(future)], -1) == 1,
            "a negative delay is not a delay"
        );

        // With a delay, old records are eligible and future ones are not.
        check!(count(&[record(old), record(old)], 1) == 2);
        check!(count(&[record(future)], 1) == 0);

        // The cutoff is `now - delay`, and only a record BETWEEN the two
        // possible cutoffs shows that: with an hour's delay a record stamped
        // now is held back, where `now + delay` would have released it. A
        // one-nanosecond delay moves the cutoff too little to tell.
        let hour_ns = 3_600 * 1_000_000_000_i64;
        check!(
            count(&[record(super::current_unix_time_ns())], hour_ns) == 0,
            "a record stamped now is newer than an hour ago"
        );

        // The first ineligible record stops the count: the second record here
        // is old enough on its own, and is still held back.
        check!(
            count(&[record(future), record(old)], 1) == 0,
            "take_while, not filter"
        );
        check!(count(&[record(old), record(future), record(old)], 1) == 1);

        check!(count(&[], 1) == 0);
        check!(count(&[], 0) == 0);
    }

    /// `apply_loki_tail_frame_limit` spends one budget across a frame's
    /// streams, and `tail_frame_is_empty` decides whether the result is worth
    /// sending at all. The two work together: the limiter drops streams it
    /// empties, so a frame limited down to nothing has no streams left and
    /// the emptiness check -- which reads the streams array, not the values
    /// inside it -- then suppresses the frame.
    #[test]
    fn a_tail_frame_limit_is_spent_across_streams_in_order() {
        let frame = |counts: &[usize]| {
            serde_json::json!({
                "streams": counts
                    .iter()
                    .map(|count| serde_json::json!({
                        "stream": {"app": "api"},
                        "values": (0..*count)
                            .map(|i| serde_json::json!([i.to_string(), "line"]))
                            .collect::<Vec<_>>(),
                    }))
                    .collect::<Vec<_>>(),
            })
        };
        let kept = |value: &serde_json::Value| {
            value["streams"]
                .as_array()
                .expect("streams is an array")
                .iter()
                .map(|stream| stream["values"].as_array().map_or(0, Vec::len))
                .collect::<Vec<_>>()
        };

        // The first stream takes 2 of the 5 and the second takes the rest.
        check!(
            kept(&super::apply_loki_tail_frame_limit(
                frame(&[2, 10]),
                Some(5)
            )) == vec![2, 3]
        );
        // A stream that exhausts the budget leaves nothing for the later ones,
        // and emptied streams are dropped rather than sent with no values --
        // the same rule as the search path.
        check!(
            kept(&super::apply_loki_tail_frame_limit(
                frame(&[5, 10]),
                Some(5)
            )) == vec![5]
        );
        check!(kept(&super::apply_loki_tail_frame_limit(frame(&[2, 2]), Some(5))) == vec![2, 2]);
        check!(kept(&super::apply_loki_tail_frame_limit(frame(&[9]), None)) == vec![9]);
        check!(
            kept(&super::apply_loki_tail_frame_limit(frame(&[9]), Some(0))).is_empty(),
            "a zero limit empties every stream, and empty streams are dropped"
        );

        // Emptiness is about the streams array, not the values in it.
        check!(super::tail_frame_is_empty(&frame(&[])));
        check!(super::tail_frame_is_empty(&serde_json::json!({})));
        check!(
            !super::tail_frame_is_empty(&frame(&[0])),
            "a stream carrying no values is still a stream"
        );
        check!(!super::tail_frame_is_empty(&frame(&[1])));
    }

    /// `consume_hot_metric_sample` spends one unit of a per-series, per-instant
    /// budget, and reports whether it could. Its three refusals are distinct
    /// causes -- the sample has no timestamp, the series and instant were never
    /// counted, or their budget is already spent -- and all three return the
    /// same false, so each is reached separately here.
    ///
    /// The decrement is the point: consuming twice from a budget of one must
    /// succeed then fail. A test that consumed once could not tell a decrement
    /// from a mere presence check.
    #[test]
    fn consuming_a_hot_metric_sample_spends_its_budget_once_per_unit() {
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        let other = Labels::default();
        let sample = serde_json::json!([1_700_000_000, "1"]);
        let key = |labels: &Labels| (labels.clone(), "1700000000".to_string());

        let mut counts = BTreeMap::new();
        counts.insert(key(&labels), 2_u64);

        // Two units budgeted, so two succeed and the third does not.
        check!(super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &sample
        ));
        check!(super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &sample
        ));
        check!(
            !super::consume_hot_metric_sample(&mut counts, &labels, &sample),
            "the budget is spent, not merely present"
        );
        check!(counts[&key(&labels)] == 0, "and it stops at zero");

        // A different series has its own budget, not this one's.
        check!(
            !super::consume_hot_metric_sample(&mut counts, &other, &sample),
            "an uncounted series has nothing to spend"
        );

        // A different instant of the SAME series likewise: the key is the pair.
        let later = serde_json::json!([1_700_000_001, "1"]);
        check!(!super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &later
        ));

        // A sample with no timestamp at all.
        check!(!super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &serde_json::json!([])
        ));
        check!(!super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &serde_json::json!("bare")
        ));
    }

    /// `loki_vector_sample_value` reads the VALUE half of an instant sample --
    /// index one, not zero -- and parses it. The timestamp beside it is also a
    /// number, so reading the wrong index yields something that parses fine and
    /// is simply wrong.
    #[test]
    fn a_loki_vector_sample_reads_its_value_and_not_its_timestamp() {
        let value = |sample: serde_json::Value| super::loki_vector_sample_value(&sample);
        let instant = |timestamp, sample_value| serde_json::json!({"metric": {}, "value": [timestamp, sample_value]});

        check!(value(instant(1_700_000_000_i64, "42")) == Some(MetricValue::new(42, 1)));
        check!(value(instant(1_700_000_000_i64, "1.5")) == Some(MetricValue::new(15, 10)));

        // The value is a STRING in Loki's encoding; a bare number is not read.
        check!(value(serde_json::json!({"value": [1, 42]})).is_none());
        // And an unparseable one is refused rather than defaulted to zero.
        check!(value(instant(1, "nonsense")).is_none());

        // Missing pieces: no value key, too short an array, not an array.
        check!(value(serde_json::json!({"metric": {}})).is_none());
        check!(value(serde_json::json!({"value": [1]})).is_none());
        check!(value(serde_json::json!({"value": "1"})).is_none());
    }

    /// `is_prometheus_duration_literal` accepts "1h30m" and refuses "30m1h":
    /// the units must run strictly from larger to smaller, which is what makes
    /// a duration unambiguous without needing to add the parts up. A repeat is
    /// refused by the same rule, since a unit is never strictly larger than
    /// itself -- which is why the ordering test is `<=` and not `<`.
    #[test]
    fn a_prometheus_duration_literal_runs_from_larger_units_to_smaller() {
        let is_duration = super::is_prometheus_duration_literal;

        // Every unit, in the one order that is allowed.
        check!(is_duration("1y2w3d4h5m6s7ms8us9ns"));
        for unit in ["y", "w", "d", "h", "m", "s", "ms", "us", "ns"] {
            check!(is_duration(&format!("1{unit}")), "{unit} alone");
        }
        check!(is_duration("1h30m"));
        check!(is_duration("90s"));
        check!(is_duration("0s"), "zero is a duration");

        // Out of order, in both the obvious and the subtle spelling. "1ms1m"
        // is the subtle one: read as text it looks ascending, but ms is the
        // SMALLER unit and so may not come first.
        check!(!is_duration("30m1h"));
        check!(!is_duration("1s1h"));
        check!(!is_duration("1ms1m"));
        check!(is_duration("1m1ms"), "that pair the right way round");
        check!(is_duration("1s1ms"), "and seconds before milliseconds");

        // A repeated unit, adjacent or separated.
        check!(!is_duration("1h1h"));
        check!(!is_duration("1h1m1h"));

        // Every chunk needs both a count and a unit.
        check!(!is_duration(""), "nothing is not a duration");
        check!(!is_duration("1"), "a bare number has no unit");
        check!(!is_duration("h"), "a bare unit has no count");
        check!(!is_duration("1h30"), "the trailing chunk has no unit");

        // Unknown units, and units that are only a prefix of a real one.
        check!(!is_duration("1x"));
        check!(!is_duration("1hh"));
        check!(!is_duration("1sec"));

        // Nothing else is allowed between chunks: no sign, no point, no space.
        check!(!is_duration("1.5h"));
        check!(!is_duration("-1h"));
        check!(!is_duration("1h "));
        check!(!is_duration("1h 30m"));
    }

    /// The in-place form of vector arithmetic: the left series is both the
    /// left operand and the output, so it is cloned before being written to.
    /// That clone is what keeps `a - b` from computing against a value it has
    /// already overwritten, and it only shows when the operator is
    /// non-commutative AND the result differs from the operand.
    #[test]
    fn in_place_vector_arithmetic_reads_the_left_operand_before_writing_it() {
        use krabka_logql::MetricScalarArithmeticOp;

        let series = |samples: &[(i64, &str)]| {
            serde_json::json!({
                "metric": {"app": "api"},
                "values": samples
                    .iter()
                    .map(|(ts, value)| serde_json::json!([ts, value]))
                    .collect::<Vec<_>>(),
            })
        };
        let pairs = |value: &serde_json::Value| {
            value
                .get("values")
                .and_then(serde_json::Value::as_array)
                .expect("the series has values")
                .iter()
                .map(|sample| {
                    (
                        sample[0].as_i64().expect("a timestamp"),
                        sample[1].as_str().expect("a value").to_string(),
                    )
                })
                .collect::<Vec<_>>()
        };
        // 2 and 3 have no right sample and are adjacent, so an index that
        // advanced on removal would keep one of them.
        let right = series(&[(1, "2"), (6, "1")]);
        let apply = |op| {
            let mut left = series(&[(1, "10"), (2, "20"), (3, "20"), (6, "7")]);
            let kept = super::apply_metric_binary_arithmetic_to_series(&mut left, &right, op);
            (kept, pairs(&left))
        };

        check!(
            apply(MetricScalarArithmeticOp::Subtract)
                == (true, vec![(1, "8".to_string()), (6, "6".to_string())]),
            "10-2 and 7-1, and the unmatched pair dropped"
        );
        check!(
            apply(MetricScalarArithmeticOp::Divide)
                == (true, vec![(1, "5".to_string()), (6, "7".to_string())])
        );

        // Everything dropped reports false so the caller can discard the
        // series rather than emit one with no samples.
        let mut orphan = series(&[(9, "1")]);
        check!(!super::apply_metric_binary_arithmetic_to_series(
            &mut orphan,
            &right,
            MetricScalarArithmeticOp::Subtract,
        ));

        // A right series with no values matches nothing at all.
        let mut left = series(&[(1, "10")]);
        check!(!super::apply_metric_binary_arithmetic_to_series(
            &mut left,
            &serde_json::json!({"metric": {}}),
            MetricScalarArithmeticOp::Subtract,
        ));

        // The instant shape, where the same clone-before-write applies to the
        // single sample.
        let instant =
            |ts: i64, value: &str| serde_json::json!({"metric": {}, "value": [ts, value]});
        let mut left = instant(1, "10");
        check!(super::apply_metric_binary_arithmetic_to_series(
            &mut left,
            &instant(1, "2"),
            MetricScalarArithmeticOp::Subtract,
        ));
        check!(left["value"][1] == "8", "10-2, not 2-10 and not 0");
    }

