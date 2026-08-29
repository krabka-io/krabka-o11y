    #[tokio::test]
    async fn querier_state_with_request_tenant_index_caches_shard_indexes_for_repeated_range() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_range = TimeRange::new(0, 100).unwrap();
        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();
        let shard_range = TimeRange::new(10, 19).unwrap();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range),
            BTreeSet::from([api]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[shard_range],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let first = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        assert_eq!(
            first.label_index.label_names(tenant),
            BTreeSet::from(["app".to_string()])
        );
        assert_eq!(
            second.label_index.label_names(tenant),
            BTreeSet::from(["app".to_string()])
        );

        let shard_prefix =
            krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
        let shard_manifest = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range,
        )
        .to_string();
        let list_count = store
            .list_prefixes()
            .into_iter()
            .filter(|prefix| prefix == &shard_prefix)
            .count();
        let shard_get_count = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest)
            .count();

        assert!(list_count == 1, "shard prefix should be listed once");
        assert!(
            shard_get_count == 1,
            "shard manifest should be fetched once"
        );
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_reuses_shard_indexes_for_moving_ranges() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let first_query_range = TimeRange::new(0, 100).unwrap();
        let moving_query_range = TimeRange::new(5, 105).unwrap();
        let shard_range_a = TimeRange::new(10, 19).unwrap();
        let shard_range_b = TimeRange::new(80, 89).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let worker =
            labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "worker")]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range_a),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 44, 45, shard_range_b),
            BTreeSet::from([worker]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[shard_range_a, shard_range_b],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let first = state
            .with_request_tenant_index(tenant, first_query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, moving_query_range)
            .await
            .unwrap();

        for state in [&first, &second] {
            check!(state.label_index.label_names(tenant) == BTreeSet::from(["app".to_string()]));
            check!(state.block_index.blocks().len() == 2);
        }

        let shard_prefix =
            krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
        let shard_manifest_a = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range_a,
        )
        .to_string();
        let shard_manifest_b = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range_b,
        )
        .to_string();
        let list_count = store
            .list_prefixes()
            .into_iter()
            .filter(|prefix| prefix == &shard_prefix)
            .count();
        let shard_get_count_a = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest_a)
            .count();
        let shard_get_count_b = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest_b)
            .count();

        check!(list_count == 1, "shard prefix should be listed once");
        check!(
            shard_get_count_a == 1,
            "shard manifest A should be fetched once"
        );
        check!(
            shard_get_count_b == 1,
            "shard manifest B should be fetched once"
        );
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_lists_shards_from_query_window_offset() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_start = 1_700_000_000_000_000_000;
        let query_end = query_start + 300_000_000_000;
        let query_range = TimeRange::new(query_start, query_end).unwrap();
        let old_shard_range =
            TimeRange::new(query_start - 600_000_000_000, query_start - 599_000_000_000).unwrap();
        let matching_shard_range = TimeRange::new(query_start + 10, query_start + 20).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 40, 41, old_shard_range),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, matching_shard_range),
            BTreeSet::from([api]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[old_shard_range, matching_shard_range],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let state = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        assert_eq!(
            state.label_index.label_names(tenant),
            BTreeSet::from(["app".to_string()])
        );
        let expected_offset =
            krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant)
                .join(format!("time={}", query_start - (query_end - query_start)))
                .to_string();
        assert!(
            store.list_offsets().contains(&expected_offset),
            "shard listing should start near the query window; offsets={:?}",
            store.list_offsets()
        );
    }

    #[test]
    fn metadata_index_range_defaults_empty_metadata_requests_to_recent_window() {
        const SIX_HOURS_NS: i64 = 6 * 60 * 60 * 1_000_000_000;
        let before = current_unix_time_ns();
        let range = metadata_index_range(&SeriesParams::default()).unwrap();
        let after = current_unix_time_ns();

        check!(
            range.start_ns >= before - SIX_HOURS_NS,
            "default metadata index start should be within Loki's default recent window"
        );
        check!(
            range.end_ns <= after,
            "default metadata index end should be now-ish, got {} after {}",
            range.end_ns,
            after
        );
        check!(
            range.end_ns - range.start_ns <= SIX_HOURS_NS,
            "default metadata index range should not be all time"
        );
    }

    #[tokio::test]
    async fn object_store_stream_query_batches_cold_block_reads() {
        let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let mut label_index = LabelIndex::default();
        let api = label_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();

        for block_id in 0_i64..4 {
            let start_ns = block_id * 10;
            let end_ns = start_ns + 9;
            let block = write_log_block_to_object_store(
                &store,
                &prefix,
                &BlockKey::new(
                    tenant,
                    0,
                    start_ns,
                    end_ns,
                    TimeRange::new(start_ns, end_ns).unwrap(),
                ),
                vec![LogRow::new(
                    api,
                    end_ns,
                    format!("api error {block_id}"),
                    BTreeMap::new(),
                )],
            )
            .await
            .unwrap();
            block_index.insert(block);
        }

        let plan = plan_stream_query(
            tenant,
            TimeRange::new(0, 39).unwrap(),
            parse_query(r#"{app="api"} |= "error""#).unwrap(),
            &label_index,
            &block_index,
        )
        .unwrap();

        let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            Arc::new(store.clone()),
            &prefix,
            &plan,
            &label_index,
            QueryHotTail {
                records: &[],
                frontier: &CompactionFrontier::new(i64::MAX),
                delete_filters: &[],
            },
            StreamScanOptions::from_stream_options(LokiDirection::Forward, Some(100), None, None),
        )
        .await
        .unwrap();

        assert_eq!(scan.scanned_blocks.len(), 4);
        assert!(
            store.max_active_gets() > 1,
            "expected cold block reads to overlap, max_active_gets={}",
            store.max_active_gets()
        );
    }

    // --- FIX B3 tests ---

    /// `connect_with_startup_retry` returns Ok immediately when the closure succeeds on the first try.
    #[tokio::test]
    async fn connect_with_startup_retry_succeeds_on_first_try() {
        let result: Result<u32, String> =
            connect_with_startup_retry("test", secs(5), secs(1), millis(1), millis(10), || async {
                Ok::<u32, String>(42)
            })
            .await;

        assert_eq!(result.unwrap(), 42);
    }

    /// `connect_with_startup_retry` retries on failure and returns Ok when a later retry succeeds.
    #[tokio::test]
    async fn connect_with_startup_retry_retries_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering as AO};
        let counter = Arc::new(AtomicU32::new(0));
        let counter2 = counter.clone();

        let result: Result<u32, String> = connect_with_startup_retry(
            "test",
            secs(10),
            secs(1),
            millis(1),
            millis(10),
            move || {
                let c = counter2.clone();
                async move {
                    let n = c.fetch_add(1, AO::SeqCst);
                    if n < 2 {
                        Err(format!("not ready yet (attempt {n})"))
                    } else {
                        Ok(99u32)
                    }
                }
            },
        )
        .await;

        assert_eq!(result.unwrap(), 99);
        assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    }

    /// `connect_with_startup_retry` returns the error after the deadline is exceeded.
    #[tokio::test]
    async fn connect_with_startup_retry_gives_up_after_deadline() {
        let result: Result<u32, String> = connect_with_startup_retry(
            "test",
            millis(50), // very short deadline
            millis(10),
            millis(1),
            millis(10),
            || async { Err::<u32, String>("always fails".to_string()) },
        )
        .await;

        assert!(result.is_err(), "expected Err after deadline");
        assert_eq!(result.unwrap_err(), "always fails");
    }

    fn acl_entry(
        resource_type: ResourceType,
        resource_name: &str,
        pattern_type: PatternType,
        principal: &str,
        operation: AclOperation,
        permission_type: PermissionType,
    ) -> AclEntry {
        AclEntry {
            resource_type,
            resource_name: resource_name.to_string(),
            pattern_type,
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission_type,
        }
    }

    #[test]
    fn missing_timestamp_fallback_age_is_exact() {
        check!(LOKI_REJECT_OLD_SAMPLES_MAX_AGE == hours(168));
    }

    #[test]
    fn distributor_policy_uses_defaults_and_cli_overrides() {
        let defaults =
            ServiceConfig::parse_from(["krabka-observability", "--target", "distributor"]);
        check!(defaults.reject_old_samples_max_age == days(7));
        check!(defaults.creation_grace_period == minutes(10));
        check!(defaults.ingest_quota_burst_window == secs(1));
        check!(defaults.wal_connect_startup_deadline == minutes(2));
        check!(defaults.wal_connect_attempt_timeout == secs(15));
        check!(defaults.wal_connect_initial_backoff == millis(200));
        check!(defaults.wal_connect_max_backoff == secs(2));

        let configured = ServiceConfig::try_parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            "--reject-old-samples-max-age=8d",
            "--creation-grace-period=11m",
            "--ingest-quota-burst-window=2s",
            "--wal-connect-startup-deadline=3m",
            "--wal-connect-attempt-timeout=16s",
            "--wal-connect-initial-backoff=300ms",
            "--wal-connect-max-backoff=3s",
        ])
        .expect("valid distributor policy");
        check!(configured.reject_old_samples_max_age == days(8));
        check!(configured.creation_grace_period == minutes(11));
        check!(configured.ingest_quota_burst_window == secs(2));
        check!(configured.wal_connect_startup_deadline == minutes(3));
        check!(configured.wal_connect_attempt_timeout == secs(16));
        check!(configured.wal_connect_initial_backoff == millis(300));
        check!(configured.wal_connect_max_backoff == secs(3));
    }

