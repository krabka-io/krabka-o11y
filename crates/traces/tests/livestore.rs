mod api_span;

use arrow::array::{Array, Int64Array, StringArray};
use assert2::check;
use datafusion::catalog::TableProvider;
use krabka_blockstore::{
    SCOL_ROOT_SPAN_NAME, SCOL_TRACE_ID, SCOL_TRACE_START_NANO, span_block_schema,
};
use krabka_traceql::{
    AttrValue as TraceqlAttrValue, Attribute, ScopedTag, TagCatalog as _, TagScope, TypedValue,
};
use krabka_traces::{
    AttrValue, EventRecord, KeyValue, LinkRecord, LiveStore, Span, SpanRecord,
    livestore::ingest_wal_payloads, querier::live::LiveSource,
};

use self::api_span::ApiSpan;

fn span(trace_id: [u8; 16], span_id: u8, start_ns: i64) -> Span {
    ApiSpan {
        trace_id,
        span_id,
        start_ns,
        duration_ns: 10,
    }
    .build()
}

fn record(tenant: &str, span: Span) -> SpanRecord {
    SpanRecord {
        tenant: tenant.into(),
        span,
    }
}

#[test]
fn assembles_recent_trace_by_id() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 2, 20)));
    store.ingest(record("tenant-a", span([2; 16], 1, 10)));
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    store.ingest(record("tenant-b", span([1; 16], 9, 5)));

    let trace = store.trace_by_id("tenant-a", &[1; 16]);
    check!(trace.iter().map(|span| span.span_id).collect::<Vec<_>>() == vec![[1; 8], [2; 8]]);
    check!(store.trace_by_id("tenant-a", &[2; 16]).len() == 1);
    check!(store.trace_by_id("tenant-b", &[1; 16]).len() == 1);
    check!(store.trace_by_id("missing", &[1; 16]).is_empty());
}

#[test]
fn evicts_spans_older_than_retention_window() {
    let mut store = LiveStore::new(50);
    store.ingest(record("tenant-a", span([1; 16], 1, 100)));
    store.ingest(record("tenant-a", span([1; 16], 2, 149)));
    store.ingest(record("tenant-a", span([1; 16], 3, 151)));

    let trace = store.trace_by_id("tenant-a", &[1; 16]);
    assert2::assert!(
        trace.iter().map(|span| span.span_id).collect::<Vec<_>>() == vec![[2; 8], [3; 8]]
    );
}

#[derive(bon::Builder, Clone, Copy)]
struct RetentionRow {
    tenant: &'static str,
    span_id: u8,
    start_ns: i64,
    name: &'static str,
}

fn retention_row(
    tenant: &'static str,
    span_id: u8,
    start_ns: i64,
    name: &'static str,
) -> RetentionRow {
    RetentionRow::builder()
        .tenant(tenant)
        .span_id(span_id)
        .start_ns(start_ns)
        .name(name)
        .build()
}

struct RetentionCase {
    retention_ns: i64,
    inputs: Vec<RetentionRow>,
    expected: Vec<RetentionRow>,
}

fn retention_case(
    retention_ns: i64,
    inputs: Vec<RetentionRow>,
    expected: Vec<RetentionRow>,
) -> RetentionCase {
    RetentionCase {
        retention_ns,
        inputs,
        expected,
    }
}

fn retention_cases() -> [RetentionCase; 8] {
    [
        retention_case(
            50,
            vec![
                retention_row("a", 1, 100, "old"),
                retention_row("a", 2, 151, "new"),
                retention_row("a", 3, 101, "first"),
                retention_row("a", 4, 100, "late-old"),
                retention_row("a", 3, 101, "second"),
            ],
            vec![
                retention_row("a", 3, 101, "first"),
                retention_row("a", 3, 101, "second"),
                retention_row("a", 2, 151, "new"),
            ],
        ),
        retention_case(
            50,
            vec![
                retention_row("a", 1, 100, "old"),
                retention_row("b", 2, 200, "new"),
                retention_row("a", 3, 150, "edge"),
                retention_row("a", 4, 149, "late-old"),
            ],
            vec![
                retention_row("a", 3, 150, "edge"),
                retention_row("b", 2, 200, "new"),
            ],
        ),
        retention_case(
            0,
            vec![
                retention_row("a", 1, 10, "old"),
                retention_row("b", 2, 10, "equal"),
                retention_row("a", 3, 11, "new"),
                retention_row("b", 4, 10, "late-old"),
                retention_row("a", 5, 11, "equal"),
            ],
            vec![
                retention_row("a", 3, 11, "new"),
                retention_row("a", 5, 11, "equal"),
            ],
        ),
        retention_case(
            -1,
            vec![
                retention_row("a", 1, i64::MIN, "sentinel"),
                retention_row("b", 2, i64::MIN, "sentinel"),
            ],
            vec![
                retention_row("a", 1, i64::MIN, "sentinel"),
                retention_row("b", 2, i64::MIN, "sentinel"),
            ],
        ),
        retention_case(
            -1,
            vec![
                retention_row("a", 1, i64::MIN, "sentinel"),
                retention_row("a", 2, i64::MIN + 1, "rejected"),
                retention_row("b", 3, i64::MIN, "late-old"),
            ],
            Vec::new(),
        ),
        retention_case(
            i64::MAX,
            vec![
                retention_row("a", 1, i64::MAX, "new"),
                retention_row("b", 2, i64::MIN, "old"),
                retention_row("a", 3, i64::MIN, "old"),
            ],
            vec![
                retention_row("a", 3, i64::MIN, "old"),
                retention_row("a", 1, i64::MAX, "new"),
                retention_row("b", 2, i64::MIN, "old"),
            ],
        ),
        retention_case(
            50,
            vec![
                retention_row("a", 1, i64::MIN + 20, "new"),
                retention_row("b", 2, i64::MIN, "edge"),
                retention_row("a", 3, i64::MIN + 10, "older"),
            ],
            vec![
                retention_row("a", 3, i64::MIN + 10, "older"),
                retention_row("a", 1, i64::MIN + 20, "new"),
                retention_row("b", 2, i64::MIN, "edge"),
            ],
        ),
        retention_case(
            i64::MIN,
            vec![
                retention_row("a", 1, 1, "rejected"),
                retention_row("a", 2, i64::MAX, "edge"),
                retention_row("b", 3, i64::MAX, "equal"),
            ],
            vec![
                retention_row("a", 2, i64::MAX, "edge"),
                retention_row("b", 3, i64::MAX, "equal"),
            ],
        ),
    ]
}

#[test]
fn wal_retention_keeps_complete_traces_at_timestamp_boundaries() {
    let make_record = |row: RetentionRow| {
        let mut value = span([1; 16], row.span_id, row.start_ns);
        value.name = row.name.into();
        record(row.tenant, value)
    };
    for case in retention_cases() {
        let retention = case.retention_ns;
        let inputs = case.inputs;
        let expected = case.expected;
        for invalid_suffix in [false, true] {
            let mut store = LiveStore::new(retention);
            let mut payloads = inputs
                .iter()
                .map(|entry| make_record(*entry).encode().unwrap())
                .collect::<Vec<_>>();
            if invalid_suffix {
                payloads.push(Vec::new());
                payloads.push(
                    make_record(retention_row("a", 9, i64::MAX, "after-error"))
                        .encode()
                        .unwrap(),
                );
            }
            let result = ingest_wal_payloads(&mut store, payloads.iter().map(Vec::as_slice));
            if invalid_suffix {
                assert2::assert!(matches!(result, Err(krabka_traces::TracesError::Wal(_))));
            } else {
                assert2::assert!(result.unwrap() == inputs.len());
            }
            for tenant in ["a", "b", "missing"] {
                let expected_trace = expected
                    .iter()
                    .filter(|entry| entry.tenant == tenant)
                    .map(|entry| make_record(*entry).span)
                    .collect::<Vec<_>>();
                assert2::assert!(store.trace_by_id(tenant, &[1; 16]) == expected_trace);
            }
        }
    }
}

// Keep the pre-index retention rule as an independent oracle, including its
// sentinel and saturating arithmetic behavior. Compare after every arrival so
// a late span, a tie, or trace recreation cannot hide an intermediate mistake.
#[test]
fn indexed_retention_matches_scan_after_every_arrival() {
    for retention in [i64::MIN, -50, -1, 0, 1, 50, 500, i64::MAX - 1, i64::MAX] {
        let mut store = LiveStore::new(retention);
        let mut expected: Vec<SpanRecord> = Vec::new();
        let mut frontier = i64::MIN;
        let mut seed = 123_u64;
        for arrival in 0..600_u64 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let time = match arrival {
                0 | 1 => i64::MIN,
                2 => i64::MIN + 1,
                3 => i64::MIN + 50,
                596 => i64::MAX - 50,
                597 => i64::MAX - 1,
                598 | 599 => i64::MAX,
                _ => i64::try_from(arrival).unwrap() - i64::try_from(seed % 80).unwrap(),
            };
            let tenant = ["a", "b", "c"][usize::try_from(seed % 3).unwrap()];
            let trace_id = [u8::try_from((seed >> 8) % 4).unwrap(); 16];
            let mut item = span(trace_id, u8::try_from(seed % 3).unwrap(), time);
            item.name = format!("arrival-{arrival}");
            if time > frontier {
                frontier = time;
                if retention != i64::MAX && frontier != i64::MIN {
                    expected.retain(|row| row.span.start_ns >= frontier.saturating_sub(retention));
                }
            }
            if retention == i64::MAX
                || frontier == i64::MIN
                || time >= frontier.saturating_sub(retention)
            {
                expected.push(record(tenant, item.clone()));
            }
            store.ingest(record(tenant, item));
            for tenant in ["a", "b", "c", "missing"] {
                for trace_id in [[0; 16], [1; 16], [2; 16], [3; 16]] {
                    let mut trace = expected
                        .iter()
                        .filter(|row| row.tenant == tenant && row.span.trace_id == trace_id)
                        .map(|row| row.span.clone())
                        .collect::<Vec<_>>();
                    trace.sort_by_key(|span| (span.start_ns, span.span_id));
                    assert2::assert!(
                        store.trace_by_id(tenant, &trace_id) == trace,
                        "retention={retention}, arrival={arrival}, tenant={tenant}"
                    );
                }
            }
        }
    }
}

#[test]
fn expiration_preserves_duplicate_order_when_an_earlier_slot_is_removed() {
    let mut store = LiveStore::new(50);
    for (start, name) in [(0, "expired"), (50, "first"), (50, "second"), (50, "third")] {
        let mut value = span([1; 16], 1, start);
        value.name = name.into();
        store.ingest(record("a", value));
    }
    store.ingest(record("b", span([2; 16], 1, 51)));
    let trace = store.trace_by_id("a", &[1; 16]);
    assert2::assert!(
        trace
            .iter()
            .map(|span| span.name.as_str())
            .collect::<Vec<_>>()
            == ["first", "second", "third"]
    );
    // Expire the entire trace, recreate it, and expire the recreated slot too.
    store.ingest(record("b", span([2; 16], 1, 101)));
    assert2::assert!(store.trace_by_id("a", &[1; 16]).is_empty());
    store.ingest(record("a", span([1; 16], 1, 100)));
    store.ingest(record("b", span([2; 16], 1, 151)));
    assert2::assert!(store.trace_by_id("a", &[1; 16]).is_empty());
}

#[tokio::test]
async fn expiration_keeps_arrival_order_for_clipped_trace_root_metadata() {
    let mut store = LiveStore::new(50);
    for (start, name) in [(0, "expired-root"), (50, "first-root"), (40, "late-root")] {
        let mut value = span([1; 16], 1, start);
        value.name = name.into();
        store.ingest(record("a", value));
    }
    store.ingest(record("b", span([2; 16], 1, 51)));
    let batches = store.span_batches("a", 50, 50).await.unwrap();
    assert2::assert!(batches.len() == 1);
    let roots = batches[0]
        .column_by_name(SCOL_ROOT_SPAN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert2::assert!(roots.iter().collect::<Vec<_>>() == [Some("first-root")]);
}

#[test]
fn exposes_recent_spans_as_mem_table_over_span_schema() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    store.ingest(record("tenant-a", span([2; 16], 1, 20)));

    let table = store.mem_table("tenant-a").unwrap();
    assert2::assert!(table.schema() == span_block_schema());
    assert2::assert!(table.schema().index_of(SCOL_TRACE_ID).is_ok());
}

#[tokio::test]
async fn live_source_exposes_trace_spans_and_tags() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    let mut child = span([1; 16], 2, 20);
    child.parent_span_id = Some([1; 8]);
    child.status_message = "retryable".into();
    child.instrumentation_scope = "otel-rust".into();
    child.instrumentation_version = "1.2.3".into();
    store.ingest(record("tenant-a", child));

    let trace = store
        .trace_spans("tenant-a", &[1; 16])
        .await
        .unwrap()
        .unwrap();
    check!(trace.root_service_name == "api");
    check!(trace.root_trace_name == "span-1");
    check!(trace.spans.len() == 2);
    check!(
        trace.spans[0].attributes
            == vec![Attribute::new(
                "http.method",
                TraceqlAttrValue::Str("GET".into())
            )]
    );

    let names = store.tag_names("tenant-a", None, 0, 100).await.unwrap();
    assert2::assert!(
        names
            .iter()
            .any(|tags| { tags.scope == TagScope::Resource && tags.tags == vec!["service.name"] })
    );
    assert2::assert!(
        names
            .iter()
            .any(|tags| tags.scope == TagScope::Span && tags.tags == vec!["http.method"])
    );
    assert_tag_scope_contains(
        &names,
        TagScope::Intrinsic,
        &["span:parentID", "span:statusMessage", "trace:duration"],
    );
    assert_tag_scope_contains(
        &names,
        TagScope::Instrumentation,
        &["instrumentation:name", "instrumentation:version"],
    );

    let values = store
        .tag_values("tenant-a", ".http.method", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&values, "string", "GET");
    let parent_ids = store
        .tag_values("tenant-a", "span:parentID", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&parent_ids, "string", "0101010101010101");
    let status_messages = store
        .tag_values("tenant-a", "span:statusMessage", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&status_messages, "string", "retryable");
    let instrumentation_names = store
        .tag_values("tenant-a", "instrumentation:name", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&instrumentation_names, "string", "otel-rust");
    let instrumentation_versions = store
        .tag_values("tenant-a", "instrumentation:version", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&instrumentation_versions, "string", "1.2.3");
    let trace_root_names = store
        .tag_values("tenant-a", "trace:rootName", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&trace_root_names, "string", "span-1");
    let trace_root_services = store
        .tag_values("tenant-a", "trace:rootService", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&trace_root_services, "string", "api");
    let trace_durations = store
        .tag_values("tenant-a", "trace:duration", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&trace_durations, "duration", "20");
}

#[tokio::test]
async fn live_trace_spans_keep_resource_attrs_out_of_span_attrs() {
    let mut store = LiveStore::new(i64::MAX);
    let mut item = span([1; 16], 1, 10);
    item.resource_attrs.push(KeyValue {
        key: "cloud.region".into(),
        value: AttrValue::Str("us-east-1".into()),
    });
    store.ingest(record("tenant-a", item));

    let trace = store
        .trace_spans("tenant-a", &[1; 16])
        .await
        .unwrap()
        .unwrap();

    assert2::assert!(
        trace.spans[0].attributes
            == vec![Attribute::new(
                "http.method",
                TraceqlAttrValue::Str("GET".into())
            )]
    );
}

fn assert_tag_scope_contains(tags: &[ScopedTag], scope: TagScope, expected: &[&str]) {
    assert2::assert!(tags.iter().any(|tags| {
        tags.scope == scope
            && expected
                .iter()
                .all(|expected| tags.tags.contains(&(*expected).to_string()))
    }));
}

fn assert_typed_value(values: &[TypedValue], type_: &str, value: &str) {
    assert2::assert!(
        values
            .iter()
            .any(|got| got.type_ == type_ && got.value == value)
    );
}

#[tokio::test]
async fn live_source_exposes_event_and_link_tags() {
    let mut store = LiveStore::new(i64::MAX);
    let mut span = span([1; 16], 1, 10);
    span.events.push(EventRecord {
        time_unix_nano: 17,
        name: "cache.miss".into(),
        attrs: vec![KeyValue {
            key: "cache.key".into(),
            value: AttrValue::Str("users/7".into()),
        }],
    });
    span.links.push(LinkRecord {
        trace_id: [2; 16],
        span_id: [3; 8],
        attrs: vec![KeyValue {
            key: "link.kind".into(),
            value: AttrValue::Str("follows-from".into()),
        }],
    });
    store.ingest(record("tenant-a", span));

    let event_tags = store
        .tag_names("tenant-a", Some(TagScope::Event), 0, 100)
        .await
        .unwrap();
    assert_tag_scope_contains(
        &event_tags,
        TagScope::Event,
        &["event:name", "event:timeSinceStart", "cache.key"],
    );
    let link_tags = store
        .tag_names("tenant-a", Some(TagScope::Link), 0, 100)
        .await
        .unwrap();
    assert_tag_scope_contains(
        &link_tags,
        TagScope::Link,
        &["link:traceID", "link:spanID", "link.kind"],
    );

    assert_typed_value(
        &store
            .tag_values("tenant-a", "event:name", 0, 100)
            .await
            .unwrap(),
        "string",
        "cache.miss",
    );
    assert_typed_value(
        &store
            .tag_values("tenant-a", "event:timeSinceStart", 0, 100)
            .await
            .unwrap(),
        "duration",
        "7",
    );
    assert_typed_value(
        &store
            .tag_values("tenant-a", "link:traceID", 0, 100)
            .await
            .unwrap(),
        "string",
        "02020202020202020202020202020202",
    );
    assert_typed_value(
        &store
            .tag_values("tenant-a", "link:spanID", 0, 100)
            .await
            .unwrap(),
        "string",
        "0303030303030303",
    );
}

#[tokio::test]
async fn live_source_batches_filter_by_time_range() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    store.ingest(record("tenant-a", span([1; 16], 2, 200)));

    let batches = store.span_batches("tenant-a", 0, 100).await.unwrap();
    check!(
        batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .collect::<Vec<_>>()
            == vec![1]
    );
    check!(store.block_builder_frontier_ns("tenant-a") == 200);
}

#[tokio::test]
async fn live_source_window_keeps_trace_level_columns_global() {
    // Root span starts at t=10 (outside the query window); a later child span
    // at t=200 falls inside the window. A window that clips the trace must not
    // make the trace-level columns reflect only the in-window subset.
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    let mut child = span([1; 16], 2, 200);
    child.parent_span_id = Some([1; 8]);
    store.ingest(record("tenant-a", child));

    // Window [150, 300] includes only the child span.
    let batches = store.span_batches("tenant-a", 150, 300).await.unwrap();
    assert2::assert!(
        batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .collect::<Vec<_>>()
            == vec![1]
    );

    let trace_start = batches[0]
        .column_by_name(SCOL_TRACE_START_NANO)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let root_name = batches[0]
        .column_by_name(SCOL_ROOT_SPAN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Trace-global start is the root span's t=10, NOT the in-window child's t=200.
    assert2::assert!(trace_start.value(0) == 10);
    // Root span name is the actual root ("span-1"), NOT the in-window child.
    assert2::assert!(root_name.value(0) == "span-1");
}

#[test]
fn ingests_encoded_wal_payloads() {
    let mut store = LiveStore::new(i64::MAX);
    let first = record("tenant-a", span([1; 16], 1, 10)).encode().unwrap();
    let second = record("tenant-a", span([1; 16], 2, 20)).encode().unwrap();

    let count = ingest_wal_payloads(&mut store, [&first[..], &second[..]]).unwrap();

    assert2::assert!(count == 2);
    assert2::assert!(store.trace_by_id("tenant-a", &[1; 16]).len() == 2);
}
