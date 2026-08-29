    /// Sorting a Loki vector result orders it by sample value, and touches
    /// nothing else: a matrix carries the same shape but must come back in the
    /// order it arrived. Nothing had called this at all, so returning without
    /// doing anything -- or sorting exactly the results it should not -- both
    /// passed.
    #[test]
    fn sorting_a_loki_vector_result_orders_only_a_vector() {
        let sample = |value: &str| serde_json::json!({"metric": {"n": value}, "value": [0, value]});
        let order = |value: &serde_json::Value| {
            value
                .pointer("/data/result")
                .and_then(serde_json::Value::as_array)
                .expect("a result array")
                .iter()
                .map(|entry| {
                    entry
                        .pointer("/metric/n")
                        .and_then(serde_json::Value::as_str)
                        .expect("a name")
                        .to_string()
                })
                .collect::<Vec<_>>()
        };

        let mut vector = serde_json::json!({
            "data": { "resultType": "vector", "result": [sample("3"), sample("1"), sample("2")] }
        });
        super::sort_loki_vector_result(&mut vector, false);
        check!(order(&vector) == vec!["1", "2", "3"], "ascending");

        super::sort_loki_vector_result(&mut vector, true);
        check!(
            order(&vector) == vec!["3", "2", "1"],
            "descending reverses it"
        );

        // Same shape, different result type: left exactly as it came.
        let mut matrix = serde_json::json!({
            "data": { "resultType": "matrix", "result": [sample("3"), sample("1")] }
        });
        super::sort_loki_vector_result(&mut matrix, false);
        check!(
            order(&matrix) == vec!["3", "1"],
            "a matrix is not reordered"
        );
    }

    /// `ingest_tenant` returns a present non-empty `X-Scope-OrgID` verbatim,
    /// but falls back to `"unknown"` when the header is missing or empty.
    #[test]
    fn ingest_tenant_reads_header_or_falls_back() {
        let mut present = HeaderMap::new();
        present.insert("X-Scope-OrgID", "acme".parse().unwrap());
        assert_eq!(ingest_tenant(&present), "acme");

        let missing = HeaderMap::new();
        assert_eq!(ingest_tenant(&missing), "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("X-Scope-OrgID", "".parse().unwrap());
        assert_eq!(ingest_tenant(&empty), "unknown");
    }

    #[tokio::test]
    async fn unavailable_query_authorizer_fails_closed() {
        let result = UnavailableQueryAuthorizer.check("tenant-a").await;

        assert2::assert!(matches!(
            result,
            Err(QueryAuthorizationError::Unavailable { tenant, .. }) if tenant == "tenant-a"
        ));
    }

    #[test]
    fn service_readiness_requires_wal_and_authorization() {
        assert2::assert!(ServiceReadiness::ready().is_ready());

        let readiness = ServiceReadiness::deferred_querier();
        assert2::assert!(!readiness.is_ready());
        readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
        assert2::assert!(!readiness.is_ready());
        readiness.wal_connected.store(false, AtomicOrdering::SeqCst);
        readiness
            .authorization_connected
            .store(true, AtomicOrdering::SeqCst);
        assert2::assert!(!readiness.is_ready());
        readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
        assert2::assert!(readiness.is_ready());
    }

    #[derive(Clone)]
    struct RecordingObjectStore {
        inner: Arc<object_store::memory::InMemory>,
        put_paths: Arc<Mutex<Vec<String>>>,
        get_paths: Arc<Mutex<Vec<String>>>,
        list_prefixes: Arc<Mutex<Vec<String>>>,
        list_offsets: Arc<Mutex<Vec<String>>>,
        get_delay: Duration,
        active_gets: Arc<std::sync::atomic::AtomicUsize>,
        max_active_gets: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RecordingObjectStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(object_store::memory::InMemory::new()),
                put_paths: Arc::new(Mutex::new(Vec::new())),
                get_paths: Arc::new(Mutex::new(Vec::new())),
                list_prefixes: Arc::new(Mutex::new(Vec::new())),
                list_offsets: Arc::new(Mutex::new(Vec::new())),
                get_delay: Duration::ZERO,
                active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn with_get_delay(mut self, get_delay: Duration) -> Self {
            self.get_delay = get_delay;
            self
        }

        fn clear_recorded_paths(&self) {
            self.put_paths.lock().unwrap().clear();
            self.get_paths.lock().unwrap().clear();
            self.list_prefixes.lock().unwrap().clear();
            self.list_offsets.lock().unwrap().clear();
        }

        fn clear_put_paths(&self) {
            self.put_paths.lock().unwrap().clear();
        }

        fn put_paths(&self) -> Vec<String> {
            self.put_paths.lock().unwrap().clone()
        }

        fn get_paths(&self) -> Vec<String> {
            self.get_paths.lock().unwrap().clone()
        }

        fn list_prefixes(&self) -> Vec<String> {
            self.list_prefixes.lock().unwrap().clone()
        }

        fn list_offsets(&self) -> Vec<String> {
            self.list_offsets.lock().unwrap().clone()
        }

        fn max_active_gets(&self) -> usize {
            self.max_active_gets
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn record_get_start(&self) {
            let active = self
                .active_gets
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let mut current = self
                .max_active_gets
                .load(std::sync::atomic::Ordering::SeqCst);
            while active > current {
                match self.max_active_gets.compare_exchange(
                    current,
                    active,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
        }

        fn record_get_end(&self) {
            self.active_gets
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl std::fmt::Debug for RecordingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RecordingObjectStore")
        }
    }

    impl std::fmt::Display for RecordingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RecordingObjectStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RecordingObjectStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.put_paths.lock().unwrap().push(location.to_string());
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.get_paths.lock().unwrap().push(location.to_string());
            self.record_get_start();
            if !self.get_delay.is_zero() {
                sleep(self.get_delay).await;
            }
            let result = self.inner.get_opts(location, options).await;
            self.record_get_end();
            result
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.list_prefixes
                .lock()
                .unwrap()
                .push(prefix.map_or_else(String::new, ToString::to_string));
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&object_store::path::Path>,
            offset: &object_store::path::Path,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.list_prefixes
                .lock()
                .unwrap()
                .push(prefix.map_or_else(String::new, ToString::to_string));
            self.list_offsets.lock().unwrap().push(offset.to_string());
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn compactor_configured_object_store_builds_when_not_injected() {
        let object_store_dir = tempfile::tempdir().unwrap();
        let object_store_url = Url::from_directory_path(object_store_dir.path())
            .expect("temporary directory should be representable as a file URL")
            .to_string();
        let config = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "compactor",
            "--object-store-url",
            &object_store_url,
        ]);

        let configured_store = build_compactor_configured_object_store(&config, None)
            .expect("valid object-store URL should configure a compactor store");

        assert!(
            configured_store.is_some(),
            "compactor should build the configured object store when no store is injected"
        );
    }

    /// The `OTLP`/HTTP logs handler must decompress `Content-Encoding: gzip`
    /// before it protobuf-decodes. The OpenTelemetry SDK's `otlphttp` exporter,
    /// which the demo's Alloy uses, gzips by default, so a regression here
    /// means every emitted log line silently fails to decode, and no logs are
    /// ingested.
    #[test]
    fn normalize_otlp_http_logs_decodes_gzip_identically_to_identity() {
        use std::io::Write as _;

        use opentelemetry_proto::tonic::{
            logs::v1::{ResourceLogs, ScopeLogs},
            resource::v1::Resource,
        };

        let request = ProtoExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![ProtoKeyValue {
                        key: "service.name".to_string(),
                        value: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::StringValue(
                                "checkout".to_string(),
                            )),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![ProtoLogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        body: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::StringValue(
                                "hello world".to_string(),
                            )),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let raw = request.encode_to_vec();

        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "demo".parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        // Identity (no Content-Encoding) decodes to a single record.
        let identity = normalize_otlp_http_logs(&headers, &raw, None, None)
            .expect("uncompressed OTLP proto logs should decode");
        assert_eq!(identity.len(), 1);
        assert_eq!(identity[0].line, "hello world");

        // The gzip-compressed body must decode to exactly the same records.
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&raw).unwrap();
        let gzipped = encoder.finish().unwrap();

        let mut gz_headers = headers.clone();
        gz_headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let from_gzip = normalize_otlp_http_logs(&gz_headers, &gzipped, None, None)
            .expect("gzip-compressed OTLP proto logs should decode");
        assert_eq!(from_gzip, identity);
    }

    fn hot_tail_test_record(timestamp_ns: i64, app: &str) -> WalLogRecord {
        WalLogRecord {
            tenant: "tenant".to_string(),
            labels: BTreeMap::from([("app".to_string(), app.to_string())]),
            timestamp_ns,
            line: format!("line@{timestamp_ns}"),
            structured_metadata: BTreeMap::new(),
            position: None,
        }
    }

    /// Brute-force oracle: the records a full linear scan keeps for an inclusive window.
    fn brute_force_in_range(
        records: &[WalLogRecord],
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<WalLogRecord> {
        records
            .iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .cloned()
            .collect()
    }

