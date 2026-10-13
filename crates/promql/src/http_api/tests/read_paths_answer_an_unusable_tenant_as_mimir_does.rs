use super::*;

/// Every row is an `X-Scope-OrgID` value that the pinned Mimir 2.16.1 image
/// was sent on `/prometheus/api/v1/query`, `/prometheus/api/v1/series`,
/// `/prometheus/api/v1/rules` and `/prometheus/config/v1/rules`, with the
/// status and the whole body it answered. Mimir rejects the tenant before the
/// Prometheus API handler runs, so the body is plain text with a line break
/// and never the JSON error envelope. A Grafana datasource shows that text to
/// its user.
#[tokio::test]
pub(crate) async fn read_paths_answer_an_unusable_tenant_as_mimir_does() {
    let too_long = "x".repeat(151);
    let too_long_and_bad = format!("{too_long}/");
    let unsupported = |tenant: &str, character: char| {
        format!("tenant ID '{tenant}' contains unsupported character '{character}'\n")
    };
    let too_many = |actual: usize| {
        format!("too many tenant IDs present in the request. max: 1 actual: {actual}\n")
    };
    struct TenantReadCase<'a> {
        name: &'a str,
        tenant: Option<&'a str>,
        status: StatusCode,
        body: String,
    }
    let rows = [
        TenantReadCase {
            name: "absent",
            tenant: None,
            status: StatusCode::UNAUTHORIZED,
            body: "no org id\n".into(),
        },
        TenantReadCase {
            name: "empty",
            tenant: Some(""),
            status: StatusCode::UNAUTHORIZED,
            body: "no org id\n".into(),
        },
        TenantReadCase {
            name: "separator",
            tenant: Some("a/b"),
            status: StatusCode::UNAUTHORIZED,
            body: unsupported("a/b", '/'),
        },
        TenantReadCase {
            name: "space",
            tenant: Some("a b"),
            status: StatusCode::UNAUTHORIZED,
            body: unsupported("a b", ' '),
        },
        TenantReadCase {
            name: "too long",
            tenant: Some(&too_long),
            status: StatusCode::UNAUTHORIZED,
            body: "tenant ID is too long: max 150 characters\n".into(),
        },
        TenantReadCase {
            name: "too long and unsupported",
            tenant: Some(&too_long_and_bad),
            status: StatusCode::UNAUTHORIZED,
            body: unsupported(&too_long_and_bad, '/'),
        },
        TenantReadCase {
            name: "dot dot",
            tenant: Some(".."),
            status: StatusCode::UNAUTHORIZED,
            body: "tenant ID is '.' or '..'\n".into(),
        },
        TenantReadCase {
            name: "two tenants",
            tenant: Some("a|b"),
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: too_many(2),
        },
        TenantReadCase {
            name: "three tenants",
            tenant: Some("a|b|c"),
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: too_many(3),
        },
        TenantReadCase {
            name: "an invalid second part",
            tenant: Some("a|b/c"),
            status: StatusCode::UNAUTHORIZED,
            body: unsupported("b/c", '/'),
        },
    ];
    let uris = [
        "/api/v1/query?query=up",
        "/prometheus/api/v1/query?query=up",
        "/api/v1/series?match[]=up",
        "/prometheus/api/v1/series?match[]=up",
        "/prometheus/api/v1/rules",
        "/prometheus/config/v1/rules",
    ];
    let router = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(two_series_store()),
        EngineOpts::default(),
    )));

    for uri in uris {
        for row in &rows {
            let mut request = Request::builder().uri(uri);
            if let Some(tenant) = row.tenant {
                request = request.header("x-scope-orgid", tenant);
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();

            assert2::check!(response.status() == row.status, "{uri} {}", row.name);
            assert2::check!(
                response
                    .headers()
                    .get(axum::http::header::CONTENT_TYPE)
                    .map(axum::http::HeaderValue::as_bytes)
                    == Some(&b"text/plain; charset=utf-8"[..]),
                "{uri} {}",
                row.name
            );
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert2::check!(
                String::from_utf8_lossy(&bytes) == row.body.as_str(),
                "{uri} {}",
                row.name
            );
        }
    }
}
