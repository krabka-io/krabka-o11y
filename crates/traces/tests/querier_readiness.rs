//! What a query-frontend's probe reads off a real querier's `/ready`.
//!
//! The querier renders its unmet gates through `krabka_observability`, and
//! `HttpReadinessProbe` parses them back into the names the fan-out reports.
//! The two are one pair. A renderer that changed its wording would leave the
//! parser saying `unnamed gate`, and a querier that is still loading its index
//! would then look the same as one that answered 503 for any other reason.
//!
//! Both ends run here over a real socket, against the router the querier role
//! serves. Tempo answers `/ready` and `/status` alike, and Grafana's datasource
//! health check reads `/status`, so every case is checked on both paths.

use std::{sync::Arc, time::Duration};

use assert2::check;
use krabka_observability::RoleReadiness;
use krabka_traceql::{EngineOpts, InMemorySpanStore, TraceqlEngine};
use krabka_traces::{
    frontend::{HttpReadinessProbe, QuerierHealth, ReadinessProbe as _},
    querier::http::{HttpConfig, router_with_config},
};

/// Serve the querier router on loopback with `readiness`, and return its
/// address.
async fn serve_querier(readiness: RoleReadiness) -> std::net::SocketAddr {
    let engine = Arc::new(TraceqlEngine::new(
        Arc::new(InMemorySpanStore::new()),
        EngineOpts::default(),
    ));
    let app = router_with_config(engine, HttpConfig::default(), readiness);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn get_text(addr: std::net::SocketAddr, path: &str) -> (reqwest::StatusCode, String) {
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    (status, resp.text().await.unwrap_or_default())
}

/// One row: which of the querier's gates are met, and what the two ends then
/// say about it.
struct Case {
    met: &'static [&'static str],
    status: reqwest::StatusCode,
    body: &'static str,
    health: QuerierHealth,
}

/// The querier's own start registers `object-store`, `trace-index`, and
/// `live-store` when it runs an embedded live tier. This walks that start.
#[tokio::test]
async fn a_querier_reports_its_gates_and_the_frontend_probe_reads_them_back() {
    let cases = [
        Case {
            met: &[],
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            body: "not ready: object-store, trace-index, live-store\n",
            health: QuerierHealth::NotReady {
                pending: "object-store, trace-index, live-store".to_string(),
            },
        },
        Case {
            met: &["object-store", "trace-index"],
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            body: "not ready: live-store\n",
            health: QuerierHealth::NotReady {
                pending: "live-store".to_string(),
            },
        },
        Case {
            met: &["object-store", "trace-index", "live-store"],
            status: reqwest::StatusCode::OK,
            body: "ready\n",
            health: QuerierHealth::Ready,
        },
    ];

    let probe = HttpReadinessProbe::new(
        Duration::from_secs(5),
        krabka_traces::frontend::QuerierScheme::Http,
        &krabka_observability::server_security::InternalClient::default(),
    )
    .unwrap();
    for case in cases {
        let readiness = RoleReadiness::new();
        for name in ["object-store", "trace-index", "live-store"] {
            let gate = readiness.gate(name);
            if case.met.contains(&name) {
                gate.mark_ready();
            }
        }
        let addr = serve_querier(readiness).await;

        for path in ["/ready", "/status"] {
            check!(
                get_text(addr, path).await == (case.status, case.body.to_string()),
                "{path} with {:?} met",
                case.met
            );
        }
        check!(
            probe.probe(&addr.to_string()).await == case.health,
            "{:?}",
            case.met
        );
    }
}

/// A port with nothing behind it is unreachable, not unready. The fan-out
/// reports the two differently, so the probe must not collapse them.
#[tokio::test]
async fn a_querier_that_is_not_there_is_unreachable_rather_than_unready() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let probe = HttpReadinessProbe::new(
        Duration::from_secs(5),
        krabka_traces::frontend::QuerierScheme::Http,
        &krabka_observability::server_security::InternalClient::default(),
    )
    .unwrap();
    let health = probe.probe(&addr.to_string()).await;
    check!(
        matches!(health, QuerierHealth::Unreachable { .. }),
        "{health:?}"
    );
}
