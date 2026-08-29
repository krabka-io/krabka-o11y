    /// The time-bucketed `records_in_range` MUST return exactly the same records (and
    /// so the same label/field sets) as a full-buffer scan, for any inclusive
    /// `[start, end]`, even though records are appended in NO timestamp order. This is the
    /// soundness guarantee that lets the query paths prune to the window instead of
    /// scanning the whole retained buffer.
    #[tokio::test]
    async fn hot_tail_records_in_range_matches_full_scan_under_out_of_order_inserts() {
        let bucket = minutes(1).nanos_i64();

        let hot_tail = BufferedLogHotTail::default();

        // Timestamps deliberately out of order and spread across many one-minute buckets,
        // with duplicates at the same instant and records straddling bucket boundaries.
        let timestamps = [
            5 * bucket + 10,
            bucket - 1, // last ns of bucket 0
            3 * bucket,
            bucket,          // first ns of bucket 1
            5 * bucket + 10, // duplicate timestamp
            0,
            7 * bucket + 42,
            2 * bucket + 999,
            3 * bucket, // duplicate timestamp in a different append position
            4 * bucket - 1,
            -bucket + 5, // a pre-epoch (negative) timestamp
            6 * bucket,
        ];
        let apps = ["api", "web", "db"];
        let records: Vec<WalLogRecord> = timestamps
            .iter()
            .enumerate()
            .map(|(i, &ts)| hot_tail_test_record(ts, apps[i % apps.len()]))
            .collect();

        // Append one at a time to exercise incremental bucket insertion of out-of-order data.
        for record in &records {
            hot_tail.append_records(vec![record.clone()]);
        }

        // `records()` must still return the full append-ordered buffer (the tail path
        // depends on this).
        assert_eq!(hot_tail.records(), records);

        // Probe a wide set of windows: exact bucket edges, sub-bucket slivers, windows
        // spanning many buckets, empty windows, and windows entirely outside the data.
        let min_ts = *timestamps.iter().min().unwrap();
        let max_ts = *timestamps.iter().max().unwrap();
        let mut probes: Vec<(i64, i64)> = Vec::new();
        // Walk window starts at a coarse quarter-bucket stride from below the earliest
        // record to above the latest, pairing each with several spans.
        let stride = bucket / 4;
        let mut start = min_ts - 2 * bucket;
        while start <= max_ts + 2 * bucket {
            for span in [0_i64, 1, bucket - 1, bucket, bucket + 1, 3 * bucket] {
                probes.push((start, start + span));
            }
            start += stride;
        }
        // Add exact per-record point windows and tight windows around each timestamp.
        for &ts in &timestamps {
            probes.push((ts, ts));
            probes.push((ts - 1, ts));
            probes.push((ts, ts + 1));
            probes.push((ts + 1, ts + 1));
        }

        for (start, end) in probes {
            if start > end {
                // Mirror the guard: an inverted window yields nothing.
                assert!(hot_tail.records_in_range(start, end).is_empty());
                continue;
            }
            let expected = brute_force_in_range(&records, start, end);
            let actual = hot_tail.records_in_range(start, end);
            assert_eq!(
                actual, expected,
                "records_in_range({start}, {end}) diverged from full-scan oracle"
            );

            // The label sets a query would derive must be identical too (records are the
            // sole input to label/field extraction).
            let expected_labels: BTreeSet<Labels> =
                expected.iter().map(|r| r.labels.clone()).collect();
            let actual_labels: BTreeSet<Labels> = actual.iter().map(|r| r.labels.clone()).collect();
            assert_eq!(
                actual_labels, expected_labels,
                "label sets diverged at [{start}, {end}]"
            );
        }

        // The trait-object path the querier actually uses must agree with the inherent method.
        let dyn_tail: Arc<dyn LogHotTail> = Arc::new(hot_tail.clone());
        let window = (2 * bucket, 6 * bucket);
        assert_eq!(
            dyn_tail.records_in_range(window.0, window.1),
            hot_tail.records_in_range(window.0, window.1),
        );

        // The default trait impl (used by other LogHotTail implementors, e.g. the
        // in-memory sink) falls back to filtering the full buffer and must also agree.
        let in_memory = InMemoryWalSink::default();
        for record in &records {
            LogWalSink::append(&in_memory, record.clone())
                .await
                .unwrap();
        }
        let in_memory_dyn: Arc<dyn LogHotTail> = Arc::new(in_memory);
        assert_eq!(
            in_memory_dyn.records_in_range(window.0, window.1),
            brute_force_in_range(&records, window.0, window.1),
        );
    }

    #[test]
    fn hot_tail_prune_compacted_rebuilds_records_and_time_index() {
        let bucket = minutes(1).nanos_i64();

        let hot_tail = BufferedLogHotTail::default();
        let mut compacted_by_offset = hot_tail_test_record(4 * bucket, "offset-old");
        compacted_by_offset.position = Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(7),
        });
        let mut kept_by_offset = hot_tail_test_record(3 * bucket, "offset-new");
        kept_by_offset.position = Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(8),
        });
        let compacted_by_time = hot_tail_test_record(2 * bucket, "time-old");
        let kept_by_time = hot_tail_test_record(5 * bucket, "time-new");
        let expected = vec![kept_by_offset.clone(), kept_by_time.clone()];

        hot_tail.append_records(vec![
            compacted_by_offset,
            kept_by_offset,
            compacted_by_time,
            kept_by_time,
        ]);

        let frontier =
            CompactionFrontier::new(2 * bucket).with_partition_offset(PartitionIndex(0), Offset(7));

        assert_eq!(hot_tail.prune_compacted(&frontier), 2);
        assert_eq!(hot_tail.records(), expected);
        assert2::assert!(hot_tail.records_in_range(0, 6 * bucket) == expected);
        assert!(hot_tail.records_in_range(2 * bucket, 2 * bucket).is_empty());
        assert!(hot_tail.records_in_range(4 * bucket, 4 * bucket).is_empty());
    }

    #[tokio::test]
    async fn compaction_frontier_refresh_prunes_hot_tail_from_object_store() {
        let store = object_store::memory::InMemory::new();
        let prefix = ObjectPath::default();
        let frontier = SharedCompactionFrontier::default();
        let hot_tail = BufferedLogHotTail::default();
        let compacted = hot_tail_test_record(1_000, "old");
        let fresh = hot_tail_test_record(3_000, "new");
        hot_tail.append_records(vec![compacted, fresh.clone()]);
        write_compaction_frontier_to_object_store(&store, &prefix, &CompactionFrontier::new(2_000))
            .await
            .unwrap();

        let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
            .await
            .unwrap();

        assert_eq!(pruned, 1);
        assert_eq!(frontier.snapshot(), CompactionFrontier::new(2_000));
        assert_eq!(hot_tail.records(), vec![fresh]);
    }

    #[tokio::test]
    async fn compaction_frontier_refresh_treats_absent_manifest_as_empty() {
        let store = object_store::memory::InMemory::new();
        let prefix = ObjectPath::default();
        let frontier = SharedCompactionFrontier::new(CompactionFrontier::new(123));
        let hot_tail = BufferedLogHotTail::default();
        let fresh = hot_tail_test_record(3_000, "new");
        hot_tail.append_records(vec![fresh.clone()]);

        let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
            .await
            .unwrap();

        assert_eq!(pruned, 0);
        assert_eq!(frontier.snapshot(), CompactionFrontier::new(123));
        assert_eq!(hot_tail.records(), vec![fresh]);
    }

    /// The shard catalog gains a compacted range once, and only once. Losing
    /// the push leaves a shard nobody can find; losing the containment test
    /// lists it twice, and the querier then reads the same shard twice.
    #[tokio::test]
    async fn the_shard_catalog_lists_each_compacted_range_exactly_once() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability");
        let tenant = "tenant-a";
        let range = TimeRange::new(300, 399).unwrap();
        let mut labels_index = LabelIndex::default();
        let api =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "api".into())]));
        let descriptor = BlockDescriptor::new(
            BlockKey::new(tenant, 0, 30, 39, range),
            BTreeSet::from([api]),
        );
        let mut block_index = BlockIndex::default();
        block_index.insert(descriptor.clone());

        for round in 1..=2 {
            write_tenant_compaction_indexes_to_object_store(
                &store,
                &prefix,
                tenant,
                &descriptor,
                &labels_index,
                &block_index,
                LogCompactionIndexOutput::FullManifestAndShardCatalog,
            )
            .await
            .unwrap();

            let catalog =
                read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, tenant)
                    .await
                    .unwrap();
            check!(catalog == vec![range], "after round {round}");
        }
    }

    #[tokio::test]
    async fn appending_log_index_shard_does_not_rewrite_historical_shards_or_full_manifest() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability");
        let tenant = "tenant-a";
        let old_range_a = TimeRange::new(100, 199).unwrap();
        let old_range_b = TimeRange::new(200, 299).unwrap();
        let new_range = TimeRange::new(300, 399).unwrap();
        let mut labels_index = LabelIndex::default();
        let api =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "api".into())]));
        let worker =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "worker".into())]));
        let admin =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "admin".into())]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 10, 19, old_range_a),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 20, 29, old_range_b),
            BTreeSet::from([worker]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[old_range_a, old_range_b],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();

        let new_descriptor = BlockDescriptor::new(
            BlockKey::new(tenant, 0, 30, 39, new_range),
            BTreeSet::from([admin]),
        );
        block_index.insert(new_descriptor.clone());
        store.clear_put_paths();

        write_tenant_compaction_indexes_to_object_store(
            &store,
            &prefix,
            tenant,
            &new_descriptor,
            &labels_index,
            &block_index,
            LogCompactionIndexOutput::ShardManifests,
        )
        .await
        .unwrap();

        // Exactly one PUT is allowed: the new shard manifest. The global
        // tenant manifest, the shard catalog, and the old shard manifests
        // must not be rewritten.
        let put_paths = store.put_paths();
        assert_eq!(
            put_paths,
            vec![
                krabka_blockstore::log_tenant_index_shard_manifest_object_path(
                    &prefix, tenant, new_range
                )
                .to_string()
            ],
            "only the new shard manifest should be written"
        );
    }

    #[test]
    fn detected_labels_empty_query_is_match_all() {
        // Grafana's Logs Drilldown loads `detected_labels?query=` with an empty
        // query to discover every label. An empty/blank query must parse to
        // `None` (match all streams), not be handed to the LogQL parser — which
        // rejects "" with `syntax error: unexpected $end, expecting '{'`.
        for raw in ["query=", "query=%20", "query=%20%20"] {
            let params = parse_detected_labels_params(Some(raw)).unwrap();
            assert!(params.query.is_none(), "{raw}: {:?}", params.query);
        }
        // A real stream selector is still preserved.
        let params = parse_detected_labels_params(Some("query=%7Bapp%3D%22api%22%7D")).unwrap();
        assert_eq!(params.query.as_deref(), Some(r#"{app="api"}"#));
    }

    #[test]
    fn instant_synthetic_vector_uses_raw_loki_timestamp() {
        let response = loki_instant_scalar_or_vector_response(
            4_000_000_000,
            ScalarVectorExpressionResult::Vector {
                sample: Some("1".to_string()),
                metric: BTreeMap::new(),
            },
        );

        assert_eq!(
            response["data"]["result"][0]["value"][0],
            json!(4_000_000_000i64)
        );
    }

    #[test]
    fn instant_scalar_expression_keeps_loki_seconds_timestamp() {
        let response = loki_instant_scalar_or_vector_response(
            4_000_000_000,
            ScalarVectorExpressionResult::Scalar {
                sample: "2".to_string(),
            },
        );

        assert_eq!(response["data"]["result"][0], json!(4));
    }

    #[test]
    fn formats_loki_numeric_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[1000000000,"non-string push timestamp"]]}]}"#;
        let timestamp = json!(1_000_000_000);
        let line = json!("non-string push timestamp");

        assert_eq!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|alues\":[[1000000000,\"non-string push timestamp\"]]}]}|...\n"
        );
    }

    #[test]
    fn formats_loki_object_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[{"ts":"1000000000"},"object push timestamp"]]}]}"#;
        let timestamp = json!({"ts": "1000000000"});
        let line = json!("object push timestamp");

        assert_eq!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|\":[[{\"ts\":\"1000000000\"},\"object push timestamp\"]]}]}|...\n"
        );
    }

    #[test]
    fn formats_loki_array_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[["1000000000"],"array push timestamp"]]}]}"#;
        let timestamp = json!(["1000000000"]);
        let line = json!("array push timestamp");

        assert_eq!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|values\":[[[\"1000000000\"],\"array push timestamp\"]]}]}|...\n"
        );
    }

    // --- FIX B1 tests ---

    /// A `TenantObjectStoreManifest` source backed by an empty in-memory
    /// store, with no manifest present, must return Ok with an empty
    /// self-clone index. It must not propagate `NotFound` as an error.
    #[tokio::test]
    async fn querier_state_with_request_tenant_index_tolerates_absent_manifest() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::default();

        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_manifest(store, prefix);

        let query_range = TimeRange::new(0, 1).unwrap();
        let result = state
            .with_request_tenant_index("test-tenant", query_range)
            .await;

        assert!(
            result.is_ok(),
            "expected Ok on absent cold index manifest, got: {:?}",
            result.err()
        );
        let returned = result.unwrap();
        assert!(
            returned.block_index.blocks().is_empty(),
            "expected empty block index when no manifest exists"
        );
    }

    /// Same check for the `TenantObjectStoreShards` variant.
    #[tokio::test]
    async fn querier_state_with_request_tenant_index_tolerates_absent_shards() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::default();

        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_shards(store, prefix);

        let query_range = TimeRange::new(0, 1).unwrap();
        let result = state
            .with_request_tenant_index("test-tenant", query_range)
            .await;

        assert!(
            result.is_ok(),
            "expected Ok on absent cold index shards, got: {:?}",
            result.err()
        );
    }

