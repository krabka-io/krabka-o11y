use assert2::check;
use krabka_units::{bytes, bytes_per_sec};

use super::{prelude::*, *};

/// The compaction span belongs to the producer's trace, taken from the
/// first record that actually carries a `traceparent`. A record without
/// one sits first on purpose: selecting it instead extracts no context and
/// leaves the batch in a trace of its own.
#[test]
pub(crate) fn a_compaction_batch_is_reparented_into_the_producers_trace() {
    use opentelemetry::trace::{TraceContextExt as _, TraceId, TracerProvider as _};
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::prelude::*;

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let tracer = provider.tracer("observability-compaction-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

    tracing::subscriber::with_default(subscriber, || {
        let record = |key: &str, value: &str| KafkaWalRecord {
            value: Vec::new(),
            partition: PartitionIndex(0),
            offset: Offset(0),
            timestamp_ms: None,
            headers: vec![KafkaWalHeader {
                key: key.to_owned(),
                value: Some(value.as_bytes().to_vec()),
            }],
        };
        let records = vec![
            record("tenant", "tenant-a"),
            record(
                "traceparent",
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            ),
        ];

        let span = tracing::info_span!("logs_compaction");
        set_remote_parent_from_wal_records(&span, &records);

        let context = span.context().span().span_context().clone();
        check!(
            context.trace_id()
                == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").expect("a trace id")
        );
    });
}

/// A dynamic tenant index is built only when the querier serves every
/// tenant *and* was pointed at a tenant index source. Either condition
/// alone leaves it reading its own index.
#[tokio::test]
pub(crate) async fn a_dynamic_tenant_index_needs_both_no_tenant_and_a_tenant_index_source() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_log_index_manifest(dir.path(), &LabelIndex::default(), &BlockIndex::default())
        .expect("seed an empty local manifest");
    let configured = ConfiguredObjectStore {
        store: Arc::new(object_store::memory::InMemory::new()),
        prefix: ObjectPath::from("observability"),
    };
    write_tenant_log_index_manifest_to_object_store(
        configured.store.as_ref(),
        &ObjectPath::from("observability/index"),
        "tenant-a",
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .await
    .expect("seed an empty tenant manifest");
    for (tenant, source, dynamic) in [
        (None, QuerierIndexSource::TenantObjectStoreManifest, true),
        (None, QuerierIndexSource::TenantObjectStoreShards, true),
        (None, QuerierIndexSource::LocalManifest, false),
        (
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
            false,
        ),
    ] {
        let config = ServiceConfig {
            tenant: tenant.map(str::to_owned),
            querier_index_source: source,
            index_prefix: Some("index".to_owned()),
            data_root: dir.path().to_path_buf(),
            ..ServiceConfig::default()
        };
        let state = build_configured_querier_state(&config, &configured)
            .await
            .expect("the configuration is valid");
        check!(
            state.dynamic_index.is_some() == dynamic,
            "{tenant:?} with {source:?}"
        );
    }
}

/// `group_right` keeps the *right* side's series and carries the named
/// labels over from the left. The match is by the `on` key, and a series
/// the comparison rejects is dropped rather than kept at either value.
#[test]
pub(crate) fn a_group_right_comparison_keeps_the_right_series_it_matched() {
    let series = |labels: Value, value: &str| json!({"metric": labels, "value": [0, value]});
    let mut left = json!({"data": {"result": [
        series(json!({"app": "api", "env": "prod"}), "5")
    ]}});
    let right = json!({"data": {"result": [
        series(json!({"app": "api", "instance": "a"}), "1"),
        series(json!({"app": "api", "instance": "b"}), "9"),
        series(json!({"app": "worker", "instance": "c"}), "0")
    ]}});
    let matching = MetricVectorMatching::On {
        labels: vec!["app".to_owned()],
        group: Some(MetricVectorGroupModifier::Right(vec!["env".to_owned()])),
    };

    apply_metric_binary_comparison_to_loki_result(
        &mut left,
        &right,
        ComparisonOp::Greater,
        false,
        Some(&matching),
    );

    // Only the right series the left one beat survives, wearing the
    // right's own labels plus `env` carried over, and the left operand's
    // value. The `worker` series matches no left key at all.
    check!(
        left == json!({"data": {"result": [{
            "metric": {"app": "api", "instance": "a", "env": "prod"},
            "value": [0, "5"]
        }]}})
    );
}

/// A single authorized tenant takes the per-tenant path, and that is the
/// only path applying `max_query_range` and the query-length limit -- the
/// multi-tenant path's scalar shortcut checks neither. Routing one tenant
/// through it would serve a query those limits refuse.
#[tokio::test]
pub(crate) async fn a_single_tenant_query_still_meets_the_configured_range_limit() {
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_max_query_range(Time::from_nanos(1_000_000_000));
    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
    let params = QueryParams {
        query: "vector(1)".to_owned(),
        time: None,
        start: Some(0),
        end: Some(10_000_000_000),
        since: None,
        step: Some(1_000_000_000),
        interval: None,
        limit: None,
        direction: None,
        delay_for: None,
    };

    let error = execute_http_query(&state, &headers, params, QueryKind::Range)
        .await
        .expect_err("ten seconds is past the one-second maximum");
    check!(matches!(error, HttpQueryError::QueryRangeTooLarge { .. }));
}

/// `vector(` written inside a string literal is just text. Telling it from
/// a real call needs the scanner to track exactly where the literal ends:
/// a scanner that leaves early reads the text as a call, and one that
/// never leaves swallows the code that follows.
#[test]
pub(crate) fn a_vector_call_inside_a_string_literal_is_not_a_signed_literal() {
    for (query, reported) in [
        ("vector(-1)", true),
        ("vector( +2)", true),
        ("vector(1)", false),
        // The call sits one character into the literal, so a scanner that
        // leaves the literal early lands on it and reports it.
        (
            r#"label_replace(vector(1), "dst", "Xvector( -1)", "s", "")"#,
            false,
        ),
        // And a real call after a literal is still reached, which a
        // scanner that never leaves one would never see.
        (
            r#"label_replace(vector(1), "a", "b", "s", "") + vector(-1)"#,
            true,
        ),
    ] {
        check!(
            signed_vector_function_literal_error(query).is_some() == reported,
            "{query}: {:?}",
            signed_vector_function_literal_error(query)
        );
    }
}

/// A delete request records its creation time in Unix *seconds*, the unit
/// Loki's own API reports. Dividing rather than taking a remainder is what
/// makes it a date at all -- a remainder is always under one second.
#[test]
pub(crate) fn a_created_delete_request_is_stamped_in_whole_seconds() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let state = CompactorDeleteState {
        delete_requests: SharedLogDeleteRequests::from_data_root(dir.path())
            .expect("an absent file is not an error"),
    };
    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));

    execute_create_delete_request(
        &state,
        &headers,
        Some(r#"query={job="api"}&start=1&end=2"#),
        &Bytes::new(),
    )
    .expect("the request is accepted");

    let requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    check!(requests.requests.len() == 1);
    let created_at = requests.requests[0].created_at;
    // Any wall clock this code runs on is past 2020, and a sub-second
    // remainder can never reach that.
    check!(created_at > 1_600_000_000, "created_at was {created_at}");
}

/// Only a *missing* delete-request file reads as "no requests"; every
/// other IO failure is an error. `refresh` then has to actually re-read
/// that file -- a compactor that never picks up a request written by
/// another process deletes nothing.
#[test]
pub(crate) fn delete_requests_reread_the_file_and_only_tolerate_it_being_absent() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let store = SharedLogDeleteRequests::from_data_root(dir.path())
        .expect("an absent file is not an error");
    check!(store.inner.lock().expect("not poisoned").next_id == 0);

    // Another process writes a request while this one holds the store.
    std::fs::write(
            log_delete_requests_path(dir.path()),
            r#"{"next_id":7,"requests":[{"tenant":"tenant-a","request_id":"r-1","query":"{job=\"api\"}","start_time":1,"end_time":2,"status":"received","created_at":3}]}"#,
        )
        .expect("write the request file");
    store.refresh().expect("the written file is readable");
    let inner = store.inner.lock().expect("not poisoned");
    check!(inner.next_id == 7);
    check!(inner.requests.len() == 1);
    check!(inner.requests[0].request_id == "r-1");
    drop(inner);

    // A directory in the file's place fails to read, and is not NotFound.
    let as_directory = dir.path().join("a-directory");
    std::fs::create_dir(&as_directory).expect("create the directory");
    let error = read_log_delete_requests(&as_directory)
        .expect_err("a directory is not a readable request file");
    check!(matches!(error, LogDeleteRequestStoreError::Io { .. }));
}

/// `loki_boltdb_shipper_compactor_running` is the one line in the status
/// page that varies by component: it reads 1 for the compactor and 0 for
/// everything else.
#[tokio::test]
pub(crate) async fn the_status_page_flags_only_the_compactor_as_running() {
    for (component, running) in [("compactor", 1), ("querier", 0), ("distributor", 0)] {
        let response = status_metrics(component);
        check!(response.status() == StatusCode::OK, "{component}");
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("the response body is readable");
        let body = String::from_utf8(bytes.to_vec()).expect("utf-8");
        check!(
            body.contains(&format!(
                "loki_boltdb_shipper_compactor_running {running}\n"
            )),
            "{component}: {body}"
        );
        check!(
            body.contains(&format!(
                "krabka_observability_service_up{{component=\"{component}\"}} 1"
            )),
            "{component}"
        );
    }
}

/// A JSON push whose value is the empty string carries no timestamp of its
/// own, so it is refused outright against the stale-sample window rather
/// than dated to the epoch. With no window configured there is nothing to
/// refuse it against.
#[test]
pub(crate) fn an_empty_json_value_is_refused_only_when_a_stale_window_is_set() {
    use krabka_units::hours;

    let labels = Labels::default();
    check!(validate_loki_empty_json_value_timestamp_window(&labels, None).is_ok());

    let error = validate_loki_empty_json_value_timestamp_window(&labels, Some(hours(1)))
        .expect_err("a configured window refuses an undated sample");
    check!(matches!(
        error,
        DistributorError::TimestampTooOldString {
            timestamp: "0001-01-01T00:00:00Z",
            ..
        }
    ));
}

/// `count_values` and `approx_topk` are both refused before a query is
/// planned. The two predicates read different aggregation fields, so a
/// query using one must not trip the other.
#[test]
pub(crate) fn count_values_and_approx_topk_are_recognised_apart_from_each_other() {
    for (query, count_values, approx_topk) in [
        (
            r#"count_values("status", rate({job="api"}[1m]))"#,
            true,
            false,
        ),
        (r#"approx_topk(3, rate({job="api"}[1m]))"#, false, true),
        (r#"sum(rate({job="api"}[1m]))"#, false, false),
        (r#"rate({job="api"}[1m])"#, false, false),
    ] {
        let parsed = parse_metric_query(query).expect(query);
        check!(
            metric_query_uses_count_values(&parsed) == count_values,
            "{query}"
        );
        check!(
            metric_query_uses_approx_topk(&parsed) == approx_topk,
            "{query}"
        );
    }
}

/// A JSON field's detected type comes from the JSON type itself, not from
/// re-parsing its rendered text. Both integer widths count as `Int` --
/// serde reports a negative one as `i64` only and a very large one as
/// `u64` only, so either alone would demote the other to `Float`.
#[test]
pub(crate) fn json_fields_take_their_detected_type_from_the_json_value() {
    let line = r#"{"a_bool":true,"neg":-1,"huge":18446744073709551615,"real":1.5,"text":"hi","nothing":null}"#;
    let mut fields = BTreeMap::new();
    detect_json_fields(&mut fields, line);

    let stats = |ty, value: &str| DetectedFieldStats {
        ty,
        values: BTreeSet::from([value.to_owned()]),
        parsers: BTreeSet::from(["json"]),
    };
    let expected = BTreeMap::from([
        (
            "a_bool".to_owned(),
            stats(DetectedFieldType::Boolean, "true"),
        ),
        ("neg".to_owned(), stats(DetectedFieldType::Int, "-1")),
        (
            "huge".to_owned(),
            stats(DetectedFieldType::Int, "18446744073709551615"),
        ),
        ("real".to_owned(), stats(DetectedFieldType::Float, "1.5")),
        ("text".to_owned(), stats(DetectedFieldType::String, "hi")),
    ]);
    check!(fields == expected);
}

/// A range query's step is refused only when it is not positive; an absent
/// one falls back to the range's own default rather than to zero.
#[test]
pub(crate) fn a_range_step_is_refused_only_when_it_is_not_positive() {
    let range = TimeRange::new(0, 60_000_000_000).unwrap();
    for (name, step, expected) in [
        (
            "a positive step is kept as given",
            Some(1_000_000_i64),
            Some(1_000_000_i64),
        ),
        ("zero is refused", Some(0), None),
        ("a negative step is refused", Some(-1), None),
        (
            "an absent step defaults off the range",
            None,
            Some(default_metric_range_step(range)),
        ),
    ] {
        check!(resolved_range_step(step, range).ok() == expected, "{name}");
    }
    check!(default_metric_range_step(range) > 0);
}

/// `max_query_range` and the `Loki` resolution cap are both inclusive: a
/// query sitting exactly on the limit is served, and only the next
/// nanosecond -- or the next point -- is refused. The boundary is the one
/// input separating `>` from `>=` in either check.
#[test]
pub(crate) fn query_range_limits_admit_the_boundary_and_refuse_the_step_past_it() {
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_max_query_range(Time::from_nanos(1_000_000));
    for (name, end_ns, allowed) in [
        ("exactly the limit", 1_000_000_i64, true),
        ("one nanosecond past it", 1_000_001, false),
    ] {
        let range = TimeRange::new(0, end_ns).unwrap();
        check!(
            validate_query_range_limit(&state, range).is_ok() == allowed,
            "{name}"
        );
    }

    // 11 000 points at a one-millisecond step is exactly the cap.
    for (name, end_ns, allowed) in [
        ("exactly the point cap", 11_000_000_000_i64, true),
        ("one point past the cap", 11_001_000_000, false),
    ] {
        let params = QueryParams {
            query: String::new(),
            time: None,
            start: None,
            end: None,
            since: None,
            step: Some(1_000_000),
            interval: None,
            limit: None,
            direction: None,
            delay_for: None,
        };
        let range = TimeRange::new(0, end_ns).unwrap();
        check!(
            validate_loki_query_range_resolution(&params, QueryKind::Range, range).is_ok()
                == allowed,
            "{name}"
        );
    }
}
