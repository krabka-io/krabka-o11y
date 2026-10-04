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
