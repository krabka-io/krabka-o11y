    #[test]
    fn acl_helpers_require_topic_operation_principal_and_pattern() {
        let allow_write = acl_entry(
            ResourceType::Topic,
            "__krabka_observability_logs_wal",
            PatternType::Literal,
            "User:tenant-a",
            AclOperation::Write,
            PermissionType::Allow,
        );
        let allow_read = acl_entry(
            ResourceType::Topic,
            "__krabka_",
            PatternType::Prefixed,
            "User:*",
            AclOperation::Read,
            PermissionType::Allow,
        );
        let deny_write = acl_entry(
            ResourceType::Topic,
            "*",
            PatternType::Literal,
            "User:tenant-a",
            AclOperation::All,
            PermissionType::Deny,
        );

        for (entry, topic, want) in [
            (&allow_write, "__krabka_observability_logs_wal", true),
            (&allow_read, "__krabka_observability_logs_wal", true),
            (&allow_read, "other-topic", false),
        ] {
            check!(
                matches_acl_topic_pattern(entry, topic) == want,
                "pattern={} topic={topic}",
                entry.resource_name
            );
        }
        // A literal "*" resource name matches any topic. Neither entry in the
        // loop above asks it: one names the topic, the other is a prefix.
        check!(matches_acl_topic_pattern(
            &deny_write,
            "some-unrelated-topic"
        ));

        check!(acl_matches_tenant_wal_write(
            &allow_write,
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));

        // The wildcard principal grants on the write side too. Only the read
        // entry above carries one, so the write side's own check was free.
        check!(acl_matches_tenant_wal_write(
            &acl_entry(
                ResourceType::Topic,
                "__krabka_observability_logs_wal",
                PatternType::Literal,
                "User:*",
                AclOperation::Write,
                PermissionType::Allow,
            ),
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));

        // And a non-Topic resource is refused reading, as it already is
        // writing.
        check!(!acl_matches_tenant_wal_read(
            &acl_entry(
                ResourceType::Group,
                "__krabka_observability_logs_wal",
                PatternType::Literal,
                "User:tenant-a",
                AclOperation::Read,
                PermissionType::Allow,
            ),
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));
        check!(acl_matches_tenant_wal_read(
            &allow_read,
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));

        // A concrete principal grants itself and nobody else. `allow_read`
        // above carries the wildcard, so its second arm answered for both and
        // nothing yet separated "this principal" from "any principal but this
        // one".
        let read_as = |principal: &str| {
            acl_entry(
                ResourceType::Topic,
                "__krabka_observability_logs_wal",
                PatternType::Literal,
                principal,
                AclOperation::Read,
                PermissionType::Allow,
            )
        };
        check!(acl_matches_tenant_wal_read(
            &read_as("User:tenant-a"),
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));
        check!(!acl_matches_tenant_wal_read(
            &read_as("User:tenant-b"),
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));
        check!(!acl_matches_tenant_wal_write(
            &allow_read,
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));
        check!(!acl_matches_tenant_wal_read(
            &allow_write,
            "User:tenant-a",
            "__krabka_observability_logs_wal"
        ));
        check!(!acl_matches_tenant_wal_write(
            &acl_entry(
                ResourceType::Group,
                "__krabka_observability_logs_wal",
                PatternType::Literal,
                "User:tenant-a",
                AclOperation::Write,
                PermissionType::Allow,
            ),
            "User:tenant-a",
            "__krabka_observability_logs_wal",
        ));
        check!(
            check_tenant_wal_write_acl(
                "tenant-a",
                "__krabka_observability_logs_wal",
                std::slice::from_ref(&allow_write)
            )
            .is_ok()
        );
        check!(
            check_tenant_wal_read_acl(
                "tenant-a",
                "__krabka_observability_logs_wal",
                std::slice::from_ref(&allow_read)
            )
            .is_ok()
        );
        check!(
            check_tenant_wal_write_acl(
                "tenant-a",
                "__krabka_observability_logs_wal",
                &[deny_write]
            )
            .is_err()
        );
        check!(
            check_tenant_wal_read_acl(
                "tenant-a",
                "__krabka_observability_logs_wal",
                &[allow_write]
            )
            .is_err()
        );
    }

    #[test]
    fn ingest_quota_bucket_and_byte_accounting_are_precise() {
        let record = WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::from([
                ("app".to_string(), "api".to_string()),
                ("env".to_string(), "prod".to_string()),
            ]),
            timestamp_ns: 42,
            line: "hello".to_string(),
            structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
            position: None,
        };
        let expected_bytes = "tenant-a".len()
            + "hello".len()
            + std::mem::size_of::<i64>()
            + "app".len()
            + "api".len()
            + "env".len()
            + "prod".len()
            + "trace_id".len()
            + "abc".len();
        check!(ingest_quota_bytes(&[record]) == measured_size(expected_bytes));

        let mut bucket = IngestQuotaBucket::new(bytes_per_sec(10), secs(1));
        check!(bucket.capacity() == bytes(10));
        check!(bucket.consume(bytes(10)));
        check!(!bucket.consume(ByteSize::from_bytes_f64(0.1)));
        bucket.update_rate(bytes_per_sec(5));
        check!(bucket.available <= bytes(5));
        bucket.available = bytes(4);
        bucket.update_rate(bytes_per_sec(20));
        check!(bucket.available >= bytes(4));
        check!(bucket.consume(bytes(4)));

        // Neither assertion above reaches the clamp: the bucket is empty by
        // then, so nothing is banked over the new capacity and `available`
        // could have been left alone -- or topped up to the new capacity --
        // without either inequality noticing.
        //
        // Lowering the rate shrinks the capacity, and what was banked above it
        // is given up.
        bucket.available = bytes(20);
        bucket.update_rate(bytes_per_sec(5));
        check!(bucket.available == bytes(5), "clamped to the new capacity");

        // Raising it grows the capacity and hands out nothing: a bucket
        // refills over time, not on a configuration change. The bound is loose
        // by a byte because `update_rate` refills first, over however long the
        // two statements took.
        bucket.available = bytes(2);
        bucket.update_rate(bytes_per_sec(50));
        check!(
            bucket.available < bytes(3),
            "not topped up to the new capacity"
        );

        // Refilling adds the rate over however long has passed. Every case
        // above reaches it only through `update_rate`, which calls it against
        // a bucket whose clock has not moved -- so with the body removed they
        // all behave the same.
        let mut refilling = IngestQuotaBucket::new(bytes_per_sec(10), secs(1));
        refilling.available = bytes(0);
        refilling.updated_at = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(500))
            .expect("the process has been running for at least half a second");
        refilling.refill();
        check!(
            refilling.available >= bytes(5),
            "half a second at ten bytes a second is at least five bytes"
        );
        check!(
            refilling.available <= bytes(10),
            "and never past the capacity"
        );

        let bucket = IngestQuotaBucket::new(bytes_per_sec(10), secs(2));
        check!(bucket.capacity() == bytes(20));
    }

    #[test]
    fn hot_tail_bucket_key_uses_euclidean_minutes() {
        let bucket_width = minutes(1);
        assert_eq!(hot_tail_bucket_key(0, bucket_width), 0);
        check!(hot_tail_bucket_key(bucket_width.nanos_i64() - 1, bucket_width) == 0);
        check!(hot_tail_bucket_key(bucket_width.nanos_i64(), bucket_width) == 1);
        assert_eq!(hot_tail_bucket_key(-1, bucket_width), -1);
        check!(hot_tail_bucket_key(-bucket_width.nanos_i64(), bucket_width) == -1);
        check!(hot_tail_bucket_key(-bucket_width.nanos_i64() - 1, bucket_width) == -2);
        check!(hot_tail_bucket_key(minutes(2).nanos_i64(), minutes(2)) == 1);
    }

    /// The buffer answers a range query through its bucket index, and nothing
    /// had exercised that path. The index is a granularity, not a filter: the
    /// exact bound is applied within the buckets it scans, so what has to hold
    /// is that no record in the window is left behind in a bucket the scan
    /// skipped.
    #[test]
    fn a_hot_tail_buffer_range_query_loses_no_record_to_its_buckets() {
        let minute = minutes(1).nanos_i64();
        let record = |timestamp_ns: i64| WalLogRecord {
            tenant: "t".to_string(),
            labels: Labels::default(),
            timestamp_ns,
            line: timestamp_ns.to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        };

        let tail = super::BufferedLogHotTail::with_bucket_width(minutes(1));
        tail.append_records(vec![record(0), record(minute), record(minute * 2)]);

        let stamps = |start: i64, end: i64| {
            tail.records_in_range(start, end)
                .into_iter()
                .map(|record| record.timestamp_ns)
                .collect::<Vec<_>>()
        };
        check!(tail.records().len() == 3, "every record is kept");
        check!(
            stamps(0, minute) == vec![0, minute],
            "both ends are inclusive"
        );
        check!(
            stamps(1, minute - 1) == Vec::<i64>::new(),
            "a window between two records holds neither"
        );
        check!(
            stamps(0, minute * 2) == vec![0, minute, minute * 2],
            "a window spanning every bucket returns every record"
        );
    }

    /// Whether a compactor run failed on the object store decides whether the
    /// run is retried, and every variant that is not one has to say so. With
    /// the classifier stuck at true, a decode failure or a missing commit
    /// position would be retried forever.
    #[test]
    fn only_an_object_store_compactor_error_is_classified_as_one() {
        use super::{CompactionFrontierStoreError, CompactorRunError};

        check!(super::compactor_run_error_is_object_store(
            &CompactorRunError::Frontier(CompactionFrontierStoreError::ObjectStore(
                object_store::Error::NotFound {
                    path: "p".to_string(),
                    source: "gone".into(),
                }
            ))
        ));
        check!(!super::compactor_run_error_is_object_store(
            &CompactorRunError::MissingCommitPosition
        ));
        check!(!super::compactor_run_error_is_object_store(
            &CompactorRunError::Frontier(CompactionFrontierStoreError::InvalidVersion {
                expected: 1,
                actual: 2,
            })
        ));
    }

    /// Accumulating a WAL batch stops on two conditions, and neither had ever
    /// been reached. An empty first poll returns straight away rather than
    /// waiting out the accumulation window for records that are not coming;
    /// and once the batch is full the loop stops, rather than taking one more
    /// poll's worth beyond the cap it was given.
    #[tokio::test]
    async fn accumulating_a_wal_batch_stops_when_empty_or_full() {
        struct ScriptedConsumer {
            batches: std::collections::VecDeque<Vec<WalRecordForTest>>,
        }
        type WalRecordForTest = super::KafkaWalRecord;

        #[async_trait]
        impl super::LogWalConsumer for ScriptedConsumer {
            async fn poll(
                &mut self,
                _timeout: Time,
            ) -> Result<Vec<super::KafkaWalRecord>, super::WalConsumerError> {
                Ok(self.batches.pop_front().unwrap_or_default())
            }
            async fn commit_compacted(
                &mut self,
                _position: super::WalPosition,
            ) -> Result<(), super::WalConsumerError> {
                Ok(())
            }
        }

        let record = |offset: i64| super::KafkaWalRecord {
            value: Vec::new(),
            partition: PartitionIndex(0),
            offset: Offset(offset),
            timestamp_ms: None,
            headers: Vec::new(),
        };
        let poll = |batches: Vec<Vec<super::KafkaWalRecord>>, max: usize| async move {
            let mut consumer = ScriptedConsumer {
                batches: batches.into_iter().collect(),
            };
            super::poll_accumulated_log_compaction_records(
                &mut consumer,
                secs(1),
                secs(5),
                millis(10),
                NonZeroUsize::new(max).expect("a positive cap"),
            )
            .await
            .expect("the scripted consumer does not fail")
        };

        // An empty first poll is the answer, not the start of a wait: the
        // batch waiting behind it must not be drawn in.
        let empty = poll(vec![vec![], vec![record(1)]], 3).await;
        check!(empty.is_empty(), "an empty poll returns empty");

        // One short of the cap accumulates; reaching the cap stops, leaving
        // the batch behind it alone.
        let full = poll(
            vec![vec![record(1)], vec![record(2), record(3)], vec![record(4)]],
            3,
        )
        .await;
        check!(full.len() == 3, "stops at the cap, got {}", full.len());
    }

    #[test]
    fn native_header_detection_requires_native_log_shape() {
        for (key, value, want) in [
            ("krabka-wal-record-type", Some(&b"log-line"[..]), true),
            ("krabka-log-timestamp-ns", Some(&b"1"[..]), true),
            ("krabka-log-label-app", Some(&b"api"[..]), true),
            ("krabka-wal-record-type", Some(&b"log"[..]), false),
            ("other", None, false),
        ] {
            let header = KafkaWalHeader {
                key: key.to_string(),
                value: value.map(<[u8]>::to_vec),
            };
            assert_eq!(has_native_kafka_log_headers(&[header]), want);
        }
    }

    #[test]
    fn varint_encoding_and_ingest_limits_pin_boundaries() {
        let mut body = Vec::new();
        encode_varint(0, &mut body);
        encode_varint(127, &mut body);
        encode_varint(128, &mut body);
        encode_varint(300, &mut body);
        assert_eq!(body, vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);

        let state = DistributorState {
            sink: Arc::new(InMemoryWalSink::default()),
            ingest_limiter: Arc::new(AllowAllIngestLimiter),
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            metrics: ServiceMetrics::new(),
            max_ingest_body: Some(bytes(5)),
            wal_append_timeout: None,
            reject_old_samples_max_age: None,
            creation_grace_period: None,
        };
        assert!(validate_ingest_body_limit(&state, bytes(5)).is_ok());
        assert!(validate_ingest_body_limit(&state, bytes(6)).is_err());
    }

    fn loki_content_type(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, value.parse().unwrap());
        headers
    }

    #[test]
    fn loki_content_type_and_body_decoding_accept_only_expected_forms() {
        let mut headers = HeaderMap::new();
        assert_eq!(decode_loki_http_body(&headers, b"raw").unwrap(), b"raw");
        headers.insert(CONTENT_ENCODING, "snappy".parse().unwrap());
        assert_eq!(decode_loki_http_body(&headers, b"raw").unwrap(), b"raw");
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, b"raw").unwrap();
        assert_eq!(
            decode_loki_http_body(&headers, &encoder.finish().unwrap()).unwrap(),
            b"raw"
        );
        headers.insert(CONTENT_ENCODING, "br".parse().unwrap());
        assert!(decode_loki_http_body(&headers, b"raw").is_err());

        for (value, want) in [
            ("application/json", Some(true)),
            ("Application/JSON; charset=utf-8", Some(true)),
            ("application/x-protobuf", Some(false)),
            ("application/json; charset", None),
            ("application/json; charset=", None),
        ] {
            assert_eq!(
                is_loki_json_content_type(&loki_content_type(value)).ok(),
                want
            );
        }
    }

