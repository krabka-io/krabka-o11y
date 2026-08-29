    /// The three per-query limits share a shape: unset means no limit, a query
    /// exactly at the limit is allowed, and one unit over is refused. Each is
    /// checked at all three points, because `>` and `>=` differ only at the
    /// boundary and "unset" is a third answer distinct from a limit of zero.
    ///
    /// They are tested together because they are parallel by design and a
    /// reader comparing them should see the same three cases each; a mutant
    /// swapping one limit's comparison for another's is caught by their
    /// carrying different values.
    #[test]
    fn every_per_query_limit_admits_exactly_its_boundary() {
        use krabka_blockstore::{BlockDescriptor, BlockKey, TimeRange};
        use krabka_logql::{StreamPlan, StreamQuery};

        let plan = |fingerprints: usize, block_bytes: &[u32]| StreamPlan {
            tenant: "tenant".to_string(),
            time_range: TimeRange::new(0, 10).expect("a valid range"),
            query: StreamQuery {
                matchers: Vec::new(),
                pipeline: Vec::new(),
            },
            fingerprints: (0..u64::try_from(fingerprints).expect("a small count")).collect(),
            blocks: block_bytes
                .iter()
                .enumerate()
                .map(|(index, size)| {
                    BlockDescriptor::new_with_size(
                        BlockKey::new(
                            "tenant",
                            0,
                            i64::try_from(index).expect("a small index"),
                            i64::try_from(index).expect("a small index"),
                            TimeRange::new(0, 10).expect("a valid range"),
                        ),
                        BTreeSet::new(),
                        krabka_units::bytes(*size),
                    )
                })
                .collect(),
        };
        let base = || super::QuerierState::new(".", LabelIndex::default(), BlockIndex::default());

        // Series: three fingerprints against a limit of three, then two.
        check!(
            super::validate_query_series_limit(&base(), &plan(3, &[])).is_ok(),
            "unset"
        );
        check!(
            super::validate_query_series_limit(&base().with_max_query_series(3), &plan(3, &[]))
                .is_ok(),
            "exactly at the limit"
        );
        check!(
            super::validate_query_series_limit(&base().with_max_query_series(2), &plan(3, &[]))
                .is_err(),
            "one over"
        );

        // Bytes: the planned total is SUMMED across blocks, so two blocks are
        // used -- one block cannot tell a sum from a maximum.
        let two_blocks = plan(0, &[40, 60]);
        check!(
            super::validate_query_bytes_limit(&base(), &two_blocks).is_ok(),
            "unset"
        );
        check!(
            super::validate_query_bytes_limit(
                &base().with_max_query_read(krabka_units::bytes(100)),
                &two_blocks,
            )
            .is_ok(),
            "exactly at the summed limit"
        );
        check!(
            super::validate_query_bytes_limit(
                &base().with_max_query_read(krabka_units::bytes(99)),
                &two_blocks,
            )
            .is_err(),
            "one byte over"
        );

        // Length: measured in bytes of the query text.
        let query = "{app=\"api\"}";
        check!(
            super::validate_query_length_limit(&base(), query).is_ok(),
            "unset"
        );
        check!(
            super::validate_query_length_limit(
                &base().with_max_query_length(krabka_units::bytes(
                    u32::try_from(query.len()).expect("a short query")
                )),
                query,
            )
            .is_ok(),
            "exactly at the limit"
        );
        check!(
            super::validate_query_length_limit(
                &base().with_max_query_length(krabka_units::bytes(
                    u32::try_from(query.len()).expect("a short query") - 1
                )),
                query,
            )
            .is_err(),
            "one byte over"
        );

        // Each refusal names its own limit rather than a shared message.
        check!(matches!(
            super::validate_query_series_limit(&base().with_max_query_series(2), &plan(3, &[])),
            Err(HttpQueryError::QuerySeriesTooLarge { .. })
        ));
        check!(matches!(
            super::validate_query_bytes_limit(
                &base().with_max_query_read(krabka_units::bytes(99)),
                &two_blocks,
            ),
            Err(HttpQueryError::QueryBytesTooLarge { .. })
        ));
        check!(matches!(
            super::validate_query_length_limit(
                &base().with_max_query_length(krabka_units::bytes(1)),
                query,
            ),
            Err(HttpQueryError::QueryLengthTooLarge { .. })
        ));
    }

    /// `validate_loki_volume_query_range_limit` caps a volume query's span at
    /// 30 days and a bit. The cap is exclusive of nothing -- a range exactly at
    /// the limit is allowed and one nanosecond more is not, which is the pair
    /// separating `>` from `>=`.
    ///
    /// A span that overflows an i64 subtraction is refused too, and reports the
    /// widest length rather than a negative one: a wrapped subtraction would
    /// otherwise report a query "shorter" than the limit and let it through.
    #[test]
    fn a_volume_query_range_is_capped_at_its_limit_exactly() {
        use krabka_blockstore::TimeRange;

        let max_ns = super::LOKI_VOLUME_MAX_QUERY_RANGE.nanos_i64();
        let range = |start_ns, end_ns| {
            super::validate_loki_volume_query_range_limit(
                TimeRange::new(start_ns, end_ns).expect("a valid range"),
            )
        };

        check!(range(0, 0).is_ok(), "an empty range is within any limit");
        check!(range(0, max_ns).is_ok(), "exactly at the limit");
        check!(range(1_000, 1_000 + max_ns).is_ok(), "wherever it starts");
        check!(range(0, max_ns + 1).is_err(), "one nanosecond over");

        // The error names how long the query actually was, so the client can
        // see by how much it missed.
        let error = range(0, max_ns + 1).expect_err("over the limit");
        check!(matches!(
            error,
            HttpQueryError::LokiQueryRangeTooLarge { .. }
        ));

        // A span that cannot be subtracted without overflowing is refused
        // rather than wrapping to a small positive number.
        check!(range(i64::MIN, i64::MAX).is_err(), "an overflowing span");
    }

    /// `validate_native_timestamp_ns` refuses a negative timestamp and returns
    /// the value otherwise. Zero is the boundary -- the Unix epoch is a real
    /// instant, so it is accepted, which is what separates `< 0` from `<= 0`.
    #[test]
    fn a_native_timestamp_may_be_the_epoch_but_not_before_it() {
        let validate = |timestamp_ns| {
            super::validate_native_timestamp_ns(timestamp_ns, timestamp_ns.to_string())
        };

        check!(validate(0).ok() == Some(0), "the epoch is a real instant");
        check!(validate(1).ok() == Some(1));
        check!(validate(i64::MAX).ok() == Some(i64::MAX));
        check!(validate(-1).is_err());
        check!(validate(i64::MIN).is_err());

        // The refusal carries the value it refused, so a log line names the
        // timestamp that was wrong rather than only that one was.
        let error = validate(-42).expect_err("negative is refused");
        check!(error.to_string().contains("-42"), "got: {error}");
    }

    /// `count_stream_map_lines` counts entries across every stream, optionally
    /// stopping before a timestamp. The bound is EXCLUSIVE, so an entry landing
    /// exactly on it is not counted -- that is the one input separating `<`
    /// from `<=`, and it matters because the same instant is the next page's
    /// first entry and would otherwise be counted twice.
    ///
    /// An entry whose timestamp will not parse IS counted. It is a line that
    /// exists, and a count used for paging must not under-report it.
    #[test]
    fn counting_stream_lines_stops_before_its_bound_but_keeps_odd_entries() {
        let streams = |entries: &[(&str, &[&str])]| {
            entries
                .iter()
                .map(|(app, timestamps)| {
                    let mut labels = Labels::default();
                    labels.insert("app".to_string(), (*app).to_string());
                    (
                        labels,
                        timestamps
                            .iter()
                            .map(|ts| [(*ts).to_string(), "line".to_string()])
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let count = super::count_stream_map_lines;

        // Unbounded: every entry across every stream.
        let two = streams(&[("api", &["1", "2", "3"]), ("web", &["4", "5"])]);
        check!(count(&two, None) == 5, "summed across streams");

        // Bounded, exclusive: 3 is counted, 4 is not.
        check!(count(&two, Some(4)) == 3);
        check!(count(&two, Some(5)) == 4, "the bound itself is excluded");
        check!(count(&two, Some(6)) == 5);
        check!(count(&two, Some(1)) == 0, "nothing before the first");

        // An unparseable timestamp is counted, bounded or not.
        let odd = streams(&[("api", &["1", "nonsense", "9"])]);
        check!(count(&odd, None) == 3);
        check!(count(&odd, Some(2)) == 2, "1 and the odd entry, but not 9");

        // Nothing to count.
        check!(count(&BTreeMap::new(), None) == 0);
        check!(count(&streams(&[("api", &[])]), None) == 0);
    }

    /// `apply_loki_stream_interval` thins a stream so consecutive entries are at
    /// least `interval` apart, keeping the first of each window. The entries
    /// straddle the boundary deliberately: one exactly AT the next allowed
    /// timestamp must be kept, since the comparison is `<` and not `<=`.
    ///
    /// An entry whose timestamp will not parse is KEPT rather than dropped --
    /// thinning is a display convenience, and silently discarding a line
    /// because its timestamp is odd would lose data the user asked for.
    #[test]
    fn a_loki_stream_interval_keeps_the_first_entry_of_each_window() {
        let stream = |timestamps: &[&str]| {
            serde_json::json!({
                "data": {"result": [{
                    "stream": {"app": "api"},
                    "values": timestamps
                        .iter()
                        .map(|ts| serde_json::json!([ts, "line"]))
                        .collect::<Vec<_>>(),
                }]}
            })
        };
        let kept = |mut value: serde_json::Value, interval| {
            super::apply_loki_stream_interval(&mut value, interval);
            value
                .pointer("/data/result")
                .and_then(serde_json::Value::as_array)
                .map(|streams| {
                    streams
                        .iter()
                        .flat_map(|s| s["values"].as_array().cloned().unwrap_or_default())
                        .map(|entry| entry[0].as_str().expect("a timestamp").to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        // Ten apart: the first is kept, then everything until ten past it.
        // 10 lands exactly on the boundary and is kept.
        check!(
            kept(stream(&["0", "5", "10", "15", "20"]), Some(10)) == vec!["0", "10", "20"],
            "an entry exactly at the boundary is kept"
        );
        check!(
            kept(stream(&["0", "9"]), Some(10)) == vec!["0"],
            "one short is dropped"
        );

        // No interval, and a zero interval, both leave the stream alone.
        check!(kept(stream(&["0", "1", "2"]), None) == vec!["0", "1", "2"]);
        check!(kept(stream(&["0", "1", "2"]), Some(0)) == vec!["0", "1", "2"]);

        // The zero-interval short circuit only shows on DESCENDING timestamps,
        // which is how Loki returns log entries. Without it a zero interval
        // sets the next allowed timestamp to the current one, and every later
        // entry compares as earlier and is dropped.
        check!(
            kept(stream(&["2", "1", "0"]), Some(0)) == vec!["2", "1", "0"],
            "a zero interval thins nothing, even newest-first"
        );

        // An unparseable timestamp is kept, and does not move the window.
        check!(
            kept(stream(&["0", "nonsense", "5"]), Some(10)) == vec!["0", "nonsense"],
            "the odd entry is kept and 5 is still inside the window"
        );

        // A stream thinned to nothing cannot happen -- the first entry always
        // survives -- but a stream that was already empty is dropped.
        let mut empty = serde_json::json!({
            "data": {"result": [{"stream": {}, "values": []}]}
        });
        super::apply_loki_stream_interval(&mut empty, Some(10));
        check!(
            empty["data"]["result"]
                .as_array()
                .expect("an array")
                .is_empty(),
            "an empty stream is dropped rather than sent"
        );
    }

    /// `parse_prometheus_duration` is the value-computing sibling of
    /// `is_prometheus_duration_literal`: same grammar, but it sums the chunks.
    /// The units must still run from larger to smaller, and a repeat is
    /// refused by that same rule.
    ///
    /// The sum is checked against a duration using several units at once,
    /// since a single-unit value cannot show a chunk being dropped or
    /// multiplied by the wrong scale.
    #[test]
    fn a_prometheus_duration_sums_its_chunks_in_nanoseconds() {
        let parse = super::parse_prometheus_duration;
        let secs = 1_000_000_000_i64;

        // Each unit's own scale.
        check!(parse("1ns") == Some(1));
        check!(parse("1us") == Some(1_000));
        check!(parse("1ms") == Some(1_000_000));
        check!(parse("1s") == Some(secs));
        check!(parse("1m") == Some(60 * secs));
        check!(parse("1h") == Some(3_600 * secs));
        check!(parse("1d") == Some(24 * 3_600 * secs));
        check!(parse("1w") == Some(7 * 24 * 3_600 * secs));
        check!(parse("1y") == Some(365 * 24 * 3_600 * secs));

        // Several units summed, so a dropped chunk changes the total.
        check!(parse("1h30m") == Some(5_400 * secs));
        check!(parse("1h1m1s") == Some(3_661 * secs));
        check!(parse("2h2m2s") == Some(7_322 * secs));
        check!(parse("1s500ms") == Some(1_500_000_000));

        // Counts above one, and zero.
        check!(parse("90s") == Some(90 * secs));
        check!(parse("0s") == Some(0));
        check!(parse("0h0m0s") == Some(0));

        // The same refusals as the literal validator.
        check!(parse("30m1h").is_none(), "out of order");
        check!(parse("1h1h").is_none(), "repeated unit");
        check!(parse("1ms1m").is_none(), "ms is the smaller unit");
        check!(parse("").is_none());
        check!(parse("1").is_none(), "no unit");
        check!(parse("h").is_none(), "no count");
        check!(parse("1x").is_none(), "unknown unit");
        check!(parse("1.5h").is_none(), "not an integer count");

        // A total that will not fit is refused rather than wrapping.
        check!(
            parse("999999999999y").is_none(),
            "overflow is not a duration"
        );
    }

    /// `hex_string` renders bytes as lower-case hex, high nibble first. The
    /// byte 0xAB is the case that matters: with a symmetric byte like 0xAA a
    /// swapped nibble order is invisible.
    #[test]
    fn hex_rendering_puts_the_high_nibble_first() {
        let hex = super::hex_string;

        check!(hex(&[0xAB]) == "ab", "high nibble first");
        check!(hex(&[0x0F]) == "0f", "a leading zero is kept");
        check!(hex(&[0xF0]) == "f0");
        check!(hex(&[0x00]) == "00");
        check!(hex(&[0xFF]) == "ff");
        check!(hex(&[0x01, 0x23]) == "0123", "bytes in order");
        check!(hex(&[]) == "");
        check!(hex(&[0xDE, 0xAD, 0xBE, 0xEF]) == "deadbeef");
    }

    /// The two error classifiers decide whether a compaction failure came from
    /// the OBJECT STORE, which is the retryable kind -- a transient 503 should
    /// be retried where a malformed block never will be. Misclassifying either
    /// way is bad in its own direction: retrying a permanent failure spins,
    /// and giving up on a transient one loses data.
    #[test]
    fn only_an_object_store_failure_is_classified_as_retryable() {
        use krabka_blockstore::LogBlockStoreError as BlockStoreError;

        let is_object_store = super::compaction_error_is_object_store;
        let object_store_error = || {
            BlockStoreError::ObjectStore(object_store::Error::NotFound {
                path: "block".to_string(),
                source: "gone".into(),
            })
        };

        // The one that is.
        check!(super::block_store_error_is_object_store(
            &object_store_error()
        ));
        check!(is_object_store(&super::CompactionError::BlockStore(
            object_store_error()
        )));

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
            check!(!super::block_store_error_is_object_store(&error), "{error}");
        }
        for error in others() {
            check!(!is_object_store(&super::CompactionError::BlockStore(error)));
        }

        // And every compaction failure that is not a block-store one at all.
        check!(!is_object_store(&super::CompactionError::EmptyWalBatch));
        check!(!is_object_store(&super::CompactionError::AllRowsDeleted));
        check!(!is_object_store(
            &super::CompactionError::MissingWalPosition { timestamp_ns: 1 }
        ));
        check!(!is_object_store(&super::CompactionError::MixedTenant {
            expected: "a".to_string(),
            actual: "b".to_string(),
        }));
        check!(!is_object_store(&super::CompactionError::MixedPartition {
            expected: 1,
            actual: 2,
        }));
    }

