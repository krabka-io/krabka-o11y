use assert2::check;
use krabka_units::convert::TimeExt as _;

use crate::tests::prelude::{
    AllowAllQueryAuthorizer, Arc, BTreeMap, BTreeSet, BlockIndex, Bytes, CompactorDeleteState,
    ComparisonOp, ComparisonResult, ConfiguredObjectStore, DetectedFieldStats, DetectedFieldType,
    DistributorError, HeaderMap, HttpQueryError, KafkaWalHeader, KafkaWalRecord, LabelIndex,
    Labels, Limits, LogDeleteRequestStoreError, MetricComparison, MetricVectorGroupModifier,
    MetricVectorMatching, ObjectPath, Offset, OverridesProvider, PartitionIndex,
    QuerierIndexSource, QuerierState, QueryKind, QueryParams, ServiceConfig,
    SharedLogDeleteRequests, StatusCode, TenantId, Time, TimeRange, Value, VectorComparison,
    apply_metric_binary_comparison_to_loki_result, build_configured_querier_state,
    default_metric_range_step, detect_json_fields, execute_create_delete_request,
    execute_http_query, json, log_delete_requests_path, metric_query_uses_approx_topk,
    metric_query_uses_count_values, parse_metric_query, read_log_delete_requests,
    resolved_range_step, set_remote_parent_from_wal_records, signed_vector_function_literal_error,
    status_metrics, validate_loki_empty_json_value_timestamp_window,
    validate_loki_query_range_resolution, validate_query_range_limit, write_log_index_manifest,
    write_tenant_log_index_manifest_to_object_store,
};

mod a_compaction_batch_is_reparented_into_the_producers_trace {
use super::*;

mod recording_trace_subscriber {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use tracing_subscriber::prelude::*;

    pub fn with_recording_trace<T>(name: &'static str, operation: impl FnOnce() -> T) -> T {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer(name);
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::with_default(subscriber, operation)
    }
}

/// The compaction span belongs to the producer's trace, taken from the
/// first record that actually carries a `traceparent`. A record without
/// one sits first on purpose: selecting it instead extracts no context and
/// leaves the batch in a trace of its own.
#[test]
pub(crate) fn a_compaction_batch_is_reparented_into_the_producers_trace() {
    use opentelemetry::trace::{TraceContextExt as _, TraceId};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    recording_trace_subscriber::with_recording_trace("observability-compaction-test", || {
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
}
mod a_vector_call_inside_a_string_literal_is_not_a_signed_literal {
use super::*;

struct VectorCallCase {
    query: &'static str,
    reported: bool,
}

/// `vector(` written inside a string literal is just text. Telling it from
/// a real call needs the scanner to track exactly where the literal ends:
/// a scanner that leaves early reads the text as a call, and one that
/// never leaves swallows the code that follows.
#[yare::parameterized(
    signed_integer = { VectorCallCase { query: "vector(-1)", reported: true } },
    spaced_positive = { VectorCallCase { query: "vector( +2)", reported: true } },
    unsigned = { VectorCallCase { query: "vector(1)", reported: false } },
    text_inside_literal = { VectorCallCase { query: r#"label_replace(vector(1), "dst", "Xvector( -1)", "s", "")"#, reported: false } },
    call_after_literal = { VectorCallCase { query: r#"label_replace(vector(1), "a", "b", "s", "") + vector(-1)"#, reported: true } },
)]
pub(crate) fn a_vector_call_inside_a_string_literal_is_not_a_signed_literal(case: VectorCallCase) {
    check!(
        signed_vector_function_literal_error(case.query).is_some() == case.reported,
        "{}: {:?}",
        case.query,
        signed_vector_function_literal_error(case.query)
    );
}
}
mod count_values_and_approx_topk_are_recognised_apart_from_each_other {
use super::*;

struct AggregationRecognitionCase {
    query: &'static str,
    count_values: bool,
    approx_topk: bool,
}

/// `count_values` and `approx_topk` are both refused before a query is
/// planned. The two predicates read different aggregation fields, so a
/// query using one must not trip the other.
#[yare::parameterized(
    count_values = { AggregationRecognitionCase { query: r#"count_values("status", rate({job="api"}[1m]))"#, count_values: true, approx_topk: false } },
    approx_topk = { AggregationRecognitionCase { query: r#"approx_topk(3, rate({job="api"}[1m]))"#, count_values: false, approx_topk: true } },
    sum = { AggregationRecognitionCase { query: r#"sum(rate({job="api"}[1m]))"#, count_values: false, approx_topk: false } },
    rate = { AggregationRecognitionCase { query: r#"rate({job="api"}[1m])"#, count_values: false, approx_topk: false } },
)]
pub(crate) fn count_values_and_approx_topk_are_recognised_apart_from_each_other(
    case: AggregationRecognitionCase,
) {
    let parsed = parse_metric_query(case.query).expect(case.query);
    check!(
        metric_query_uses_count_values(&parsed) == case.count_values,
        "{}",
        case.query
    );
    check!(
        metric_query_uses_approx_topk(&parsed) == case.approx_topk,
        "{}",
        case.query
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
        query_authorizer: Arc::new(AllowAllQueryAuthorizer),
    };
    let tenant = TenantId::new("tenant-a").expect("a valid tenant id");

    execute_create_delete_request(
        &state,
        &crate::tests::prelude::RequestSecurity::unauthenticated(),
        &tenant,
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
        VectorComparison {
            comparison: MetricComparison {
                op: ComparisonOp::Greater,
                result: ComparisonResult::Filter,
            },
            matching: Some(&matching),
        },
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

struct DynamicIndexCase {
    tenant: Option<&'static str>,
    source: QuerierIndexSource,
    expected_dynamic: bool,
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
    for case in [
        DynamicIndexCase {
            tenant: None,
            source: QuerierIndexSource::TenantObjectStoreManifest,
            expected_dynamic: true,
        },
        DynamicIndexCase {
            tenant: None,
            source: QuerierIndexSource::TenantObjectStoreShards,
            expected_dynamic: true,
        },
        DynamicIndexCase {
            tenant: None,
            source: QuerierIndexSource::LocalManifest,
            expected_dynamic: false,
        },
        DynamicIndexCase {
            tenant: Some("tenant-a"),
            source: QuerierIndexSource::TenantObjectStoreManifest,
            expected_dynamic: false,
        },
    ] {
        let config = ServiceConfig {
            tenant: case.tenant.map(str::to_owned),
            querier_index_source: case.source,
            index_prefix: Some("index".to_owned()),
            data_root: dir.path().to_path_buf(),
            ..ServiceConfig::default()
        };
        let state = build_configured_querier_state(
            &config,
            &configured,
            Arc::new(OverridesProvider::new(Limits::default())),
        )
        .await
        .expect("the configuration is valid");
        check!(
            state.dynamic_index.is_some() == case.expected_dynamic,
            "{:?} with {:?}",
            case.tenant,
            case.source
        );
    }
}

struct RangeStepCase {
    name: &'static str,
    step: Option<i64>,
    expected: Option<i64>,
}

/// A range query's step is refused only when it is not positive; an absent
/// one falls back to the range's own default rather than to zero.
#[test]
pub(crate) fn a_range_step_is_refused_only_when_it_is_not_positive() {
    let range = TimeRange::new(0, 60_000_000_000).unwrap();
    for case in [
        RangeStepCase {
            name: "a positive step is kept as given",
            step: Some(1_000_000_i64),
            expected: Some(1_000_000_i64),
        },
        RangeStepCase {
            name: "zero is refused",
            step: Some(0),
            expected: None,
        },
        RangeStepCase {
            name: "a negative step is refused",
            step: Some(-1),
            expected: None,
        },
        RangeStepCase {
            name: "an absent step defaults off the range",
            step: None,
            expected: Some(default_metric_range_step(range)),
        },
    ] {
        check!(
            resolved_range_step(case.step, range).ok() == case.expected,
            "{}",
            case.name
        );
    }
    check!(default_metric_range_step(range) > 0);
}

/// A single authorized tenant takes the per-tenant path, and that is the
/// only path applying `max_query_range` and the query-length limit -- the
/// multi-tenant path's scalar shortcut checks neither. Routing one tenant
/// through it would serve a query those limits refuse.
#[tokio::test]
pub(crate) async fn a_single_tenant_query_still_meets_the_configured_range_limit() {
    let state =
        QuerierState::new(".", LabelIndex::default(), BlockIndex::default()).with_limits(Limits {
            max_query_range: Time::from_nanos(1_000_000_000),
            ..Limits::default()
        });
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

    let error = execute_http_query(
        &state,
        &crate::tests::prelude::RequestSecurity::unauthenticated(),
        &headers,
        params,
        QueryKind::Range,
    )
    .await
    .expect_err("ten seconds is past the one-second maximum");
    check!(matches!(error, HttpQueryError::QueryRangeTooLarge { .. }));
}

/// A JSON push whose value is the empty string carries no timestamp of its
/// own, so it is refused outright against the stale-sample window rather
/// than dated to the epoch. With no window configured there is nothing to
/// refuse it against.
#[test]
pub(crate) fn an_empty_json_value_is_refused_only_when_a_stale_window_is_set() {
    use krabka_units::hours;

    let labels = Labels::default();
    check!(validate_loki_empty_json_value_timestamp_window(&labels, Time::ZERO).is_ok());

    let error = validate_loki_empty_json_value_timestamp_window(&labels, hours(1))
        .expect_err("a configured window refuses an undated sample");
    check!(matches!(
        error,
        DistributorError::TimestampTooOldString {
            timestamp: "0001-01-01T00:00:00Z",
            ..
        }
    ));
}

struct ExpectedField {
    name: &'static str,
    stats: DetectedFieldStats,
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
    let expected_fields = [
        ExpectedField {
            name: "a_bool",
            stats: stats(DetectedFieldType::Boolean, "true"),
        },
        ExpectedField {
            name: "neg",
            stats: stats(DetectedFieldType::Int, "-1"),
        },
        ExpectedField {
            name: "huge",
            stats: stats(DetectedFieldType::Int, "18446744073709551615"),
        },
        ExpectedField {
            name: "real",
            stats: stats(DetectedFieldType::Float, "1.5"),
        },
        ExpectedField {
            name: "text",
            stats: stats(DetectedFieldType::String, "hi"),
        },
    ];
    let mut expected = BTreeMap::new();
    for field in expected_fields {
        expected.insert(field.name.to_owned(), field.stats);
    }
    check!(fields == expected);
}

struct QueryRangeLimitCase {
    name: &'static str,
    end_ns: i64,
    allowed: bool,
}

/// `max_query_range` and the `Loki` resolution cap are both inclusive: a
/// query sitting exactly on the limit is served, and only the next
/// nanosecond -- or the next point -- is refused. The boundary is the one
/// input separating `>` from `>=` in either check.
#[test]
pub(crate) fn query_range_limits_admit_the_boundary_and_refuse_the_step_past_it() {
    let state =
        QuerierState::new(".", LabelIndex::default(), BlockIndex::default()).with_limits(Limits {
            max_query_range: Time::from_nanos(1_000_000),
            ..Limits::default()
        });
    for case in [
        QueryRangeLimitCase {
            name: "exactly the limit",
            end_ns: 1_000_000_i64,
            allowed: true,
        },
        QueryRangeLimitCase {
            name: "one nanosecond past it",
            end_ns: 1_000_001,
            allowed: false,
        },
    ] {
        let range = TimeRange::new(0, case.end_ns).unwrap();
        check!(
            validate_query_range_limit(&state, range).is_ok() == case.allowed,
            "{}",
            case.name
        );
    }

    // 11 000 points at a one-millisecond step is exactly the cap.
    for case in [
        QueryRangeLimitCase {
            name: "exactly the point cap",
            end_ns: 11_000_000_000_i64,
            allowed: true,
        },
        QueryRangeLimitCase {
            name: "one point past the cap",
            end_ns: 11_001_000_000,
            allowed: false,
        },
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
        let range = TimeRange::new(0, case.end_ns).unwrap();
        check!(
            validate_loki_query_range_resolution(&params, QueryKind::Range, range).is_ok()
                == case.allowed,
            "{}",
            case.name
        );
    }
}

struct ComponentMetricCase {
    component: &'static str,
    running: u8,
}

/// `loki_boltdb_shipper_compactor_running` is the one line in the status
/// page that varies by component. It reads 1 on a process that runs `Loki`'s
/// compactor, which here is the block builder and the all-in-one that
/// contains it, and 0 on the roles that do not write durable storage.
#[tokio::test]
pub(crate) async fn the_status_page_flags_only_the_compactor_as_running() {
    for case in [
        ComponentMetricCase {
            component: "compactor",
            running: 1,
        },
        ComponentMetricCase {
            component: "all",
            running: 1,
        },
        ComponentMetricCase {
            component: "querier",
            running: 0,
        },
        ComponentMetricCase {
            component: "distributor",
            running: 0,
        },
    ] {
        let response = status_metrics(case.component);
        check!(response.status() == StatusCode::OK, "{}", case.component);
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("the response body is readable");
        let body = String::from_utf8(bytes.to_vec()).expect("utf-8");
        check!(
            body.contains(&format!(
                "loki_boltdb_shipper_compactor_running {}\n",
                case.running
            )),
            "{}: {body}",
            case.component
        );
        check!(
            body.contains(&format!(
                "krabka_observability_service_up{{component=\"{}\"}} 1",
                case.component
            )),
            "{}",
            case.component
        );
    }
}
