//! traces -> profiles: a span id read out of a stored trace selects that span's
//! profile.
//!
//! `SelectMergeSpanProfile` is Pyroscope's span-scoped profile query: given a
//! set of span ids, it merges only the profile samples that were taken inside
//! those spans. `krabka-profiles` implements it and routes it, and an in-crate
//! test covers its tree encoding — but that test invents its span id, so
//! nothing has ever shown that an id `krabka-traces` stores is an id
//! `krabka-profiles` can be queried with.
//!
//! This suite takes the id the long way round:
//!
//! ```text
//! Span -> LiveStore::ingest -> LiveStore::trace_by_id   (krabka-traces)
//!      -> span.span_id: [u8; 8]
//!      -> u64::from_be_bytes                            [the join]
//!      -> WalSample::span_id -> build_block -> ColdProfileStore
//!      -> POST /querier.v1.QuerierService/SelectMergeSpanProfile
//!                                                       (krabka-profiles)
//! ```
//!
//! The span id is never written down twice. It is read back out of the traces
//! store and carried into the profiles query, so a change to either side's
//! representation breaks this test rather than passing silently.
//!
//! # The join is not shared code
//!
//! `krabka-traces` holds a span id as `[u8; 8]`; `krabka-profiles` holds it as
//! `u64` and its selector parses decimal or `0x`-hex strings. Nothing in the
//! workspace converts between the two: the profiles OTLP decoder does its own
//! `u64::from_be_bytes` privately, and that big-endian reading is the only
//! thing making the two halves agree. This suite performs the same conversion,
//! and the negative case below is what keeps it honest — a selector that
//! matched everything would pass the positive assertion on its own.

use std::sync::Arc;

use assert2::{assert, check};
use axum::http::{Request, StatusCode};
use krabka_blockstore::{BlockIndex as _, LABEL_PROFILE_TYPE, Labels, ProfileIndex};
use krabka_profiles::{
    ProfileRecord, WalSample,
    blockbuilder::{STACKTRACE_PARTITION, build_block},
    cold_store::ColdProfileStore,
    limits::Limits,
    query::{QuerierState, router},
    wal::{WalFunction, WalLocation, WalMapping, WalSymbolSet},
};
use krabka_units::{Time, convert::TimeExt as _};
use object_store::{ObjectStore, memory::InMemory};
use serde_json::{Value, json};
use tower::ServiceExt as _;

const TENANT: &str = "tenant-a";
const SERVICE: &str = "api";
const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
/// The frame the span's profile samples are attributed to. Naming it here is
/// what lets the assertion say the *right* profile came back, not merely a
/// non-empty one.
const FRAME: &str = "main.work";

const TRACE_ID: [u8; 16] = [
    0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36,
];
/// The span whose profile is wanted, and its sibling, which is the control. The
/// two differ so that selecting one must exclude the other.
const WANTED_SPAN_ID: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
const OTHER_SPAN_ID: [u8; 8] = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];

const SAMPLE_MS: i64 = 20;

fn span(span_id: [u8; 8], name: &str) -> krabka_traces::Span {
    krabka_traces::Span {
        trace_id: TRACE_ID,
        span_id,
        parent_span_id: None,
        name: name.to_string(),
        kind: krabka_traces::SpanKind::Server,
        start_ns: SAMPLE_MS * 1_000_000,
        duration_ns: 5_000_000,
        status: krabka_traces::StatusCode::Ok,
        status_message: String::new(),
        resource_attrs: Vec::new(),
        span_attrs: Vec::new(),
        events: Vec::new(),
        links: Vec::new(),
        instrumentation_scope: String::new(),
        instrumentation_version: String::new(),
    }
}

/// One profiles WAL record: a single `main.work` frame, sampled once inside
/// each of the two spans.
///
/// `span_id` is a `u64` here because that is how `krabka-profiles` stores it,
/// all the way down to the `span_id` Arrow column the query filters on.
fn profile_record(span_ids: [u64; 2]) -> ProfileRecord {
    ProfileRecord {
        tenant: TENANT.to_string(),
        // `__profile_type__` is not decoration: `ProfileIndex::add_series`
        // registers a series under a profile type only when this label is
        // present, and the query resolves its type selector through that
        // registration. Ingest's `split` inserts it for exactly this reason.
        labels: vec![
            (LABEL_PROFILE_TYPE.to_string(), PROFILE_TYPE.to_string()),
            ("__name__".to_string(), "process_cpu".to_string()),
            ("service_name".to_string(), SERVICE.to_string()),
        ],
        profile_type: PROFILE_TYPE.to_string(),
        samples: span_ids
            .into_iter()
            .map(|span_id| WalSample {
                stacktrace_location_refs: vec![0],
                value: 7,
                timestamp_ns: SAMPLE_MS * 1_000_000,
                span_id: Some(span_id),
                trace_id: Some(TRACE_ID.to_vec()),
            })
            .collect(),
        symbols: WalSymbolSet {
            strings: vec![String::new(), FRAME.to_string()],
            functions: vec![WalFunction {
                name: 1,
                system_name: 1,
                filename: 0,
                start_line: 0,
            }],
            locations: vec![WalLocation {
                address: 0x1000,
                mapping_id: 0,
                lines: vec![(0, 10)],
            }],
            mappings: vec![WalMapping {
                memory_start: 0,
                memory_limit: 0,
                file_offset: 0,
                filename: 0,
                build_id: 0,
                has_functions: true.into(),
                has_filenames: false.into(),
                has_line_numbers: false.into(),
                has_inline_frames: false.into(),
            }],
        },
    }
}

/// Writes one profiles block into an in-memory object store and returns a
/// querier over it, wired exactly as the block-builder wires the real one.
async fn querier_over(record: &ProfileRecord) -> axum::Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();

    let labels = Labels::from_pairs(record.labels.iter().cloned());
    index.add_series(TENANT, labels.fingerprint(), &labels);

    let metas = build_block(&store, TENANT, 0, std::slice::from_ref(record), (0, 0))
        .await
        .expect("build profiles block");
    assert!(!metas.is_empty());
    for meta in &metas {
        index.add_block(meta);
        index.add_profile_block(&meta.tenant, &meta.object_key, vec![STACKTRACE_PARTITION]);
    }

    let cold = ColdProfileStore::new(store, Arc::new(index));
    router(Arc::new(QuerierState::new_with_limits(
        Arc::new(cold),
        Limits {
            // The query below spans milliseconds, but the default ceiling is
            // measured against wall-clock-shaped ranges. Zero is "no ceiling".
            max_query_length: Time::ZERO,
            ..Limits::default()
        },
    )))
}

/// Issues `SelectMergeSpanProfile` for a set of span selectors and returns the
/// merged flamegraph's frame names and total.
///
/// The total is what makes the selector observable: each span contributed one
/// sample worth 7, so the total says how many of them the query merged.
async fn span_profile(app: axum::Router, span_selectors: &[String]) -> (Vec<String>, i64) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/querier.v1.QuerierService/SelectMergeSpanProfile")
                .header("content-type", "application/json")
                .header("x-scope-orgid", TENANT)
                .body(axum::body::Body::from(
                    json!({
                        "profileTypeID": PROFILE_TYPE,
                        "labelSelector": format!("{{service_name=\"{SERVICE}\"}}"),
                        "spanSelector": span_selectors,
                        "start": 0,
                        "end": 100,
                    })
                    .to_string(),
                ))
                .expect("span profile request"),
        )
        .await
        .expect("span profile response");

    assert!(response.status() == StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("span profile body");
    let json: Value = serde_json::from_slice(&body).expect("span profile json");

    let names = json["flamegraph"]["names"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|name| name.as_str().map(str::to_string))
        .collect();
    // Connect's JSON encoding renders an int64 as a string; a zero total is
    // omitted from the message altogether.
    let total = json["flamegraph"]["total"]
        .as_str()
        .map(|total| total.parse::<i64>().expect("total is an integer"))
        .or_else(|| json["flamegraph"]["total"].as_i64())
        .unwrap_or(0);
    (names, total)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_span_id_stored_by_traces_selects_that_span_s_profile() {
    // --- traces: store a trace, then read the span id back out of it -------
    let mut live = krabka_traces::LiveStore::new(i64::MAX);
    for span_id in [WANTED_SPAN_ID, OTHER_SPAN_ID] {
        live.ingest(krabka_traces::SpanRecord {
            tenant: TENANT.to_string(),
            span: span(span_id, "GET /orders"),
        });
    }

    let stored = live.trace_by_id(TENANT, &TRACE_ID);
    assert!(stored.len() == 2);

    // Every id from here on comes out of the traces store. Nothing re-states
    // the constant, so a traces-side change to how a span id is held shows up
    // as a failure of the profiles query below.
    let wanted = stored
        .iter()
        .find(|span| span.span_id == WANTED_SPAN_ID)
        .expect("the wanted span is in the stored trace");
    let other = stored
        .iter()
        .find(|span| span.span_id == OTHER_SPAN_ID)
        .expect("the control span is in the stored trace");

    // The join: traces holds `[u8; 8]`, profiles holds `u64`, big-endian.
    let wanted_id = u64::from_be_bytes(wanted.span_id);
    let other_id = u64::from_be_bytes(other.span_id);
    assert!(wanted_id != other_id);

    // --- profiles: store a profile sampled inside both spans ---------------
    let record = profile_record([wanted_id, other_id]);

    // --- the query, three ways --------------------------------------------
    //
    // Each span contributed one sample worth 7. Selecting both must merge both,
    // selecting one must merge one, and selecting neither must merge none. Only
    // the three together show that `span_selector` is what decides: the middle
    // case alone would also pass if the query silently returned a fixed subset,
    // and the last alone would also pass if it always returned nothing.
    let both = [wanted_id.to_string(), other_id.to_string()];
    let (names, total) = span_profile(querier_over(&record).await, &both).await;
    check!(names.iter().any(|name| name == FRAME));
    check!(total == 14);

    let (names, total) = span_profile(querier_over(&record).await, &[wanted_id.to_string()]).await;
    check!(names.iter().any(|name| name == FRAME));
    check!(total == 7);

    let (names, total) = span_profile(querier_over(&record).await, &[other_id.to_string()]).await;
    check!(names.iter().any(|name| name == FRAME));
    check!(total == 7);

    // A span id that no sample carries. The profile is not merely smaller, it
    // is absent: the frame is gone with it.
    let (names, total) =
        span_profile(querier_over(&record).await, &["999999999".to_string()]).await;
    check!(!names.iter().any(|name| name == FRAME));
    check!(total == 0);
}
