//! Standalone Krabka traces querier, preloaded with a multi-service demo
//! fixture.
//!
//! Use it to point a real Grafana Tempo datasource at Krabka and run complex
//! `TraceQL` queries by hand, or through a headless browser.
//!
//! It serves the Tempo query API on `0.0.0.0:3201`. `KRABKA_DEMO_ADDR`
//! overrides that address.
//!
//! Run: `cargo run -p krabka-traces --example grafana_demo`
//!
//! The fixture is timestamped at "a few minutes ago", so it falls inside
//! Grafana's default "last 1 hour" window. These queries are all valid against
//! the engine:
//!   * `{ resource.service.name = "checkout-frontend" }`   (root-service selector)
//!   * `{ span:status = error }`                            (error spans)
//!   * `{ span:duration > 100ms }`                          (slow spans)
//!   * `{ .http.method = "GET" } >> { .db.system = "postgresql" }`  (structural)
//!   * `{ .http.method = "POST" && span:status = error }`  (boolean span filter)
//!   * `{ resource.service.name = "checkout-frontend" } | rate()`  (metrics)

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::Request,
    middleware::{self, Next},
    response::Response as AxumResponse,
};
use krabka_traceql::{AttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine};
use krabka_traces::querier::http::router;
use krabka_units::{Time, convert::TimeExt as _};

const TENANT: &str = "anonymous";
// OTLP span kinds.
const SERVER: i32 = 2;
const CLIENT: i32 = 3;
// OTLP status codes.
const OK: i32 = 1;
const ERROR: i32 = 2;

fn base_ns() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    i64::try_from(now).unwrap_or(i64::MAX) - 180_000_000_000 // 3 minutes ago
}

#[derive(Clone, Copy)]
#[allow(clippy::struct_field_names)]
struct SpanIdentity {
    trace_id: u8,
    span_id: u8,
    parent_span_id: Option<u8>,
}

#[derive(Clone, Copy)]
struct SpanTiming {
    start_offset_ms: i64,
    duration_ms: i64,
}

#[derive(Clone, Copy)]
struct SpanStatus {
    code: i32,
    message: &'static str,
}

struct SpanAttributeInput {
    key: &'static str,
    value: AttrValue,
}

fn span(
    base: i64,
    identity: SpanIdentity,
    name: &str,
    kind: i32,
    timing_ms: SpanTiming,
    status: SpanStatus,
    attrs: Vec<SpanAttributeInput>,
) -> InputSpan {
    InputSpan {
        trace_id: [identity.trace_id; 16],
        span_id: [identity.span_id; 8],
        parent_span_id: identity.parent_span_id.map(|p| [p; 8]),
        name: name.to_string(),
        kind,
        start_unix_nano: base + timing_ms.start_offset_ms * 1_000_000,
        duration: Time::from_millis(timing_ms.duration_ms),
        status_code: status.code,
        status_message: status.message.to_string(),
        instrumentation_name: "krabka-demo".to_string(),
        instrumentation_version: "1.0.0".to_string(),
        attrs: attrs
            .into_iter()
            .map(|attribute| krabka_traceql::Attribute::new(attribute.key, attribute.value))
            .collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

fn http_get_span(base: i64, identity: SpanIdentity, name: &str, timing: SpanTiming) -> InputSpan {
    span(
        base,
        identity,
        name,
        SERVER,
        timing,
        SpanStatus {
            code: OK,
            message: "",
        },
        vec![
            SpanAttributeInput {
                key: "http.method",
                value: str_attr("GET"),
            },
            SpanAttributeInput {
                key: "http.status_code",
                value: AttrValue::Int(200),
            },
        ],
    )
}

fn str_attr(value: &str) -> AttrValue {
    AttrValue::Str(value.to_string())
}

async fn log_req(req: Request, next: Next) -> AxumResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let accept = req
        .headers()
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let resp = next.run(req).await;
    println!("{method} {uri} accept=[{accept}] -> {}", resp.status());
    resp
}

#[allow(clippy::too_many_lines)]
fn demo_store() -> InMemorySpanStore {
    let base = base_ns();
    let mut store = InMemorySpanStore::new();

    // Trace 1 — checkout flow with a failed payment leg (5 spans, 3 services).
    store.push_trace(
        TENANT,
        "checkout-frontend",
        "GET /checkout",
        vec![
            span(
                base,
                SpanIdentity {
                    trace_id: 0x11,
                    span_id: 0x02,
                    parent_span_id: None,
                },
                "GET /checkout",
                SERVER,
                SpanTiming {
                    start_offset_ms: 0,
                    duration_ms: 120,
                },
                SpanStatus {
                    code: OK,
                    message: "",
                },
                vec![
                    SpanAttributeInput {
                        key: "http.method",
                        value: str_attr("GET"),
                    },
                    SpanAttributeInput {
                        key: "http.target",
                        value: str_attr("/checkout"),
                    },
                    SpanAttributeInput {
                        key: "http.status_code",
                        value: AttrValue::Int(200),
                    },
                ],
            ),
            span(
                base,
                SpanIdentity {
                    trace_id: 0x11,
                    span_id: 0x03,
                    parent_span_id: Some(0x02),
                },
                "cart.rpc",
                CLIENT,
                SpanTiming {
                    start_offset_ms: 5,
                    duration_ms: 45,
                },
                SpanStatus {
                    code: OK,
                    message: "",
                },
                vec![
                    SpanAttributeInput {
                        key: "rpc.system",
                        value: str_attr("grpc"),
                    },
                    SpanAttributeInput {
                        key: "peer.service",
                        value: str_attr("cart-backend"),
                    },
                ],
            ),
            span(
                base,
                SpanIdentity {
                    trace_id: 0x11,
                    span_id: 0x04,
                    parent_span_id: Some(0x03),
                },
                "cart.lookup",
                SERVER,
                SpanTiming {
                    start_offset_ms: 10,
                    duration_ms: 30,
                },
                SpanStatus {
                    code: OK,
                    message: "",
                },
                vec![
                    SpanAttributeInput {
                        key: "db.system",
                        value: str_attr("postgresql"),
                    },
                    SpanAttributeInput {
                        key: "db.statement",
                        value: str_attr("SELECT * FROM cart WHERE user=$1"),
                    },
                ],
            ),
            span(
                base,
                SpanIdentity {
                    trace_id: 0x11,
                    span_id: 0x05,
                    parent_span_id: Some(0x02),
                },
                "POST /charge",
                CLIENT,
                SpanTiming {
                    start_offset_ms: 55,
                    duration_ms: 70,
                },
                SpanStatus {
                    code: ERROR,
                    message: "payment declined",
                },
                vec![
                    SpanAttributeInput {
                        key: "http.method",
                        value: str_attr("POST"),
                    },
                    SpanAttributeInput {
                        key: "http.status_code",
                        value: AttrValue::Int(402),
                    },
                    SpanAttributeInput {
                        key: "peer.service",
                        value: str_attr("payment-svc"),
                    },
                ],
            ),
            span(
                base,
                SpanIdentity {
                    trace_id: 0x11,
                    span_id: 0x06,
                    parent_span_id: Some(0x05),
                },
                "payment.charge",
                SERVER,
                SpanTiming {
                    start_offset_ms: 60,
                    duration_ms: 55,
                },
                SpanStatus {
                    code: ERROR,
                    message: "card_declined",
                },
                vec![
                    SpanAttributeInput {
                        key: "db.system",
                        value: str_attr("mysql"),
                    },
                    SpanAttributeInput {
                        key: "payment.provider",
                        value: str_attr("stripe"),
                    },
                ],
            ),
        ],
    );

    // Trace 2 — product browse (2 spans).
    store.push_trace(
        TENANT,
        "web-frontend",
        "GET /products",
        vec![
            http_get_span(
                base,
                SpanIdentity {
                    trace_id: 0x22,
                    span_id: 0x02,
                    parent_span_id: None,
                },
                "GET /products",
                SpanTiming {
                    start_offset_ms: 0,
                    duration_ms: 80,
                },
            ),
            span(
                base,
                SpanIdentity {
                    trace_id: 0x22,
                    span_id: 0x03,
                    parent_span_id: Some(0x02),
                },
                "inventory.list",
                SERVER,
                SpanTiming {
                    start_offset_ms: 8,
                    duration_ms: 25,
                },
                SpanStatus {
                    code: OK,
                    message: "",
                },
                vec![SpanAttributeInput {
                    key: "db.system",
                    value: str_attr("postgresql"),
                }],
            ),
        ],
    );

    // Trace 3 — fast checkout (2 spans, also checkout-frontend rooted).
    store.push_trace(
        TENANT,
        "checkout-frontend",
        "GET /checkout",
        vec![
            http_get_span(
                base,
                SpanIdentity {
                    trace_id: 0x33,
                    span_id: 0x02,
                    parent_span_id: None,
                },
                "GET /checkout",
                SpanTiming {
                    start_offset_ms: 0,
                    duration_ms: 35,
                },
            ),
            span(
                base,
                SpanIdentity {
                    trace_id: 0x33,
                    span_id: 0x03,
                    parent_span_id: Some(0x02),
                },
                "cart.lookup",
                SERVER,
                SpanTiming {
                    start_offset_ms: 4,
                    duration_ms: 15,
                },
                SpanStatus {
                    code: OK,
                    message: "",
                },
                vec![SpanAttributeInput {
                    key: "db.system",
                    value: str_attr("postgresql"),
                }],
            ),
        ],
    );

    // Trace 4 — slow analytics report (2 spans, both > 100ms).
    store.push_trace(
        TENANT,
        "reporting-svc",
        "GET /report",
        vec![
            http_get_span(
                base,
                SpanIdentity {
                    trace_id: 0x44,
                    span_id: 0x02,
                    parent_span_id: None,
                },
                "GET /report",
                SpanTiming {
                    start_offset_ms: 0,
                    duration_ms: 450,
                },
            ),
            span(
                base,
                SpanIdentity {
                    trace_id: 0x44,
                    span_id: 0x03,
                    parent_span_id: Some(0x02),
                },
                "analytics.aggregate",
                SERVER,
                SpanTiming {
                    start_offset_ms: 20,
                    duration_ms: 400,
                },
                SpanStatus {
                    code: OK,
                    message: "",
                },
                vec![SpanAttributeInput {
                    key: "db.system",
                    value: str_attr("clickhouse"),
                }],
            ),
        ],
    );

    // Trace 5 — failed login (1 span, POST + error).
    store.push_trace(
        TENANT,
        "auth-svc",
        "POST /login",
        vec![span(
            base,
            SpanIdentity {
                trace_id: 0x55,
                span_id: 0x02,
                parent_span_id: None,
            },
            "POST /login",
            SERVER,
            SpanTiming {
                start_offset_ms: 0,
                duration_ms: 60,
            },
            SpanStatus {
                code: ERROR,
                message: "invalid credentials",
            },
            vec![
                SpanAttributeInput {
                    key: "http.method",
                    value: str_attr("POST"),
                },
                SpanAttributeInput {
                    key: "http.status_code",
                    value: AttrValue::Int(401),
                },
            ],
        )],
    );

    store
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("KRABKA_DEMO_ADDR").unwrap_or_else(|_| "0.0.0.0:3201".to_string());
    let engine = Arc::new(TraceqlEngine::new(
        Arc::new(demo_store()),
        EngineOpts::default(),
    ));
    let app = router(engine).layer(middleware::from_fn(log_req));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("krabka traces demo querier listening on http://{addr}");
    println!("tenant: {TENANT} (no X-Scope-OrgID header required)");
    println!(
        "trace ids: 11..(checkout+error)  22..(browse)  33..(fast checkout)  44..(slow report)  55..(failed login)"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
