use arrow::{array::AsArray, datatypes::Int64Type, record_batch::RecordBatch};
use krabka_blockstore::{
    SCOL_ROOT_SERVICE_NAME, SCOL_ROOT_SPAN_NAME, SCOL_TRACE_DURATION_NANOS, SCOL_TRACE_START_NANO,
};

use super::*;

#[tokio::test]
async fn packing_clipped_traces_keeps_each_roots_columns_and_payload() {
    let mut store = LiveStore::new(i64::MAX);
    for (trace, service, duration) in [(1, "api", 500), (2, "web", 800)] {
        let mut root = span_with_everything();
        root.trace_id = [trace; 16];
        root.span_id = [1; 8];
        root.name = format!("{service}-root");
        root.resource_attrs[0].value = AttrValue::Str(service.into());
        let mut child = root.clone();
        child.span_id = [2; 8];
        child.parent_span_id = Some(root.span_id);
        child.name = format!("{service}-child");
        child.start_ns = 2_000;
        child.duration_ns = duration;
        for span in [root, child] {
            store.ingest(SpanRecord {
                tenant: "t".into(),
                span,
            });
        }
    }
    store.ingest(SpanRecord {
        tenant: "other".into(),
        span: span_with_everything(),
    });
    let batches = store.span_batches("t", 2_000, 2_400).await.unwrap();
    check!(batches.len() == 1);
    let batch = &batches[0];
    check!(batch.num_rows() == 2);
    let strings = |name| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>()
    };
    let integers = |name| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Int64Type>()
            .values()
            .to_vec()
    };
    check!(strings(SCOL_ROOT_SERVICE_NAME) == vec![Some("api"), Some("web")]);
    check!(strings(SCOL_ROOT_SPAN_NAME) == vec![Some("api-root"), Some("web-root")]);
    check!(integers(SCOL_TRACE_START_NANO) == vec![1_000, 1_000]);
    check!(integers(SCOL_TRACE_DURATION_NANOS) == vec![1_500, 1_800]);

    let encoded = crate::querier::live::encode_span_batches(&batches).unwrap();
    let decoded = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(encoded), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    check!(decoded == batches);
    check!(
        store
            .span_batches("t", 3_000, 4_000)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn many_small_traces_use_bounded_batches() {
    let mut store = LiveStore::new(i64::MAX);
    for trace in 1_u128..=8_193 {
        let mut span = span_with_everything();
        span.trace_id = trace.to_be_bytes();
        store.ingest(SpanRecord {
            tenant: "t".into(),
            span,
        });
    }
    let batches = store.span_batches("t", 0, 10_000).await.unwrap();
    check!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>()
            == vec![8_192, 1]
    );
}

#[tokio::test]
async fn borrowed_window_keeps_complete_rows_order_and_owned_output() {
    use krabka_blockstore::{
        AttrValue as Value, NestedSet, SpanAttr, SpanEvent, SpanKind as Kind, SpanLink, SpanRow,
        StatusCode as Status, encode_span_rows,
    };
    use krabka_units::{Time, convert::TimeExt as _};

    let mut root = span_with_everything();
    root.span_id = [1; 8];
    root.name = "root".into();
    let mut store = LiveStore::new(i64::MAX);
    // Out-of-order arrival and equal timestamps must still sort by span id.
    for id in [9, 8] {
        let mut child = root.clone();
        child.span_id = [id; 8];
        child.parent_span_id = Some([1; 8]);
        child.name = format!("child-{id}");
        child.start_ns = 2_000;
        child.span_attrs.extend([
            KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("POST".into()),
            },
            KeyValue {
                key: "__resource.service.name".into(),
                value: AttrValue::Str("spoof".into()),
            },
            KeyValue {
                key: "bytes".into(),
                value: AttrValue::Bytes(vec![0, 255]),
            },
            KeyValue {
                key: "empty".into(),
                value: AttrValue::Array(Vec::new()),
            },
            KeyValue {
                key: "one".into(),
                value: AttrValue::Array(vec![AttrValue::Int(7)]),
            },
            KeyValue {
                key: "many".into(),
                value: AttrValue::Array(vec![AttrValue::Bool(true), AttrValue::Bool(false)]),
            },
            KeyValue {
                key: "mixed".into(),
                value: AttrValue::Array(vec![AttrValue::Int(7), AttrValue::Str("seven".into())]),
            },
        ]);
        store.ingest(SpanRecord {
            tenant: "t".into(),
            span: child,
        });
    }
    store.ingest(SpanRecord {
        tenant: "t".into(),
        span: root.clone(),
    });
    store.ingest(SpanRecord {
        tenant: "other".into(),
        span: root,
    });

    let expected = [8, 9].map(|id| SpanRow {
        trace_id: [1; 16],
        span_id: [id; 8],
        parent_span_id: Some([1; 8]),
        nested_set: NestedSet {
            nested_set_left: if id == 8 { 1 } else { 3 },
            nested_set_right: if id == 8 { 2 } else { 4 },
            parent_id: -1,
        },
        child_count: 0,
        root_service_name: Some("api".into()),
        root_span_name: Some("root".into()),
        trace_start_unix_nano: 1_000,
        trace_duration: Time::from_nanos(1_500),
        name: Some(format!("child-{id}")),
        kind: Kind::Server,
        start_unix_nano: 2_000,
        duration: Time::from_nanos(500),
        status_code: Status::Ok,
        status_message: Some(String::new()),
        instrumentation_name: Some("otel-rust".into()),
        instrumentation_version: Some("1.2.3".into()),
        attrs: vec![
            SpanAttr {
                key: "__resource.service.name".into(),
                is_array: false,
                value: Value::Str(vec!["api".into()]),
            },
            SpanAttr {
                key: "http.method".into(),
                is_array: true,
                value: Value::Str(vec!["GET".into(), "POST".into()]),
            },
            SpanAttr {
                key: "bytes".into(),
                is_array: false,
                value: Value::Unsupported(r#"{"bytesValue":"AP8="}"#.into()),
            },
            SpanAttr {
                key: "empty".into(),
                is_array: true,
                value: Value::Str(Vec::new()),
            },
            SpanAttr {
                key: "one".into(),
                is_array: true,
                value: Value::Int(vec![7]),
            },
            SpanAttr {
                key: "many".into(),
                is_array: true,
                value: Value::Bool(vec![true, false]),
            },
            SpanAttr {
                key: "mixed".into(),
                is_array: false,
                value: Value::Unsupported(
                    r#"{"arrayValue":{"values":[{"intValue":"7"},{"stringValue":"seven"}]}}"#
                        .into(),
                ),
            },
        ],
        events: vec![SpanEvent {
            name: "exception".into(),
            time_since_start: Time::from_nanos(-900),
            attrs: vec![SpanAttr {
                key: "exception.type".into(),
                is_array: false,
                value: Value::Str(vec!["timeout".into()]),
            }],
        }],
        links: vec![SpanLink {
            linked_trace_id: [9; 16],
            linked_span_id: [8; 8],
            attrs: vec![SpanAttr {
                key: "link.kind".into(),
                is_array: false,
                value: Value::Str(vec!["retry".into()]),
            }],
        }],
    });
    // Compare every Arrow column with an independent row ledger.
    let batches = store.span_batches("t", 2_000, 2_000).await.unwrap();
    check!(batches == vec![encode_span_rows(&expected).unwrap()]);
    check!(
        store
            .span_batches("t", 2_001, 3_000)
            .await
            .unwrap()
            .is_empty()
    );
    check!(
        store
            .span_batches("missing", 0, 3_000)
            .await
            .unwrap()
            .is_empty()
    );
    // Borrowing the input must not borrow the returned payload from the store.
    store.by_tenant.clear();
    check!(batches == vec![encode_span_rows(&expected).unwrap()]);
}
