use super::*;

async fn parts_for_test(response: Response) -> (StatusCode, String, Vec<u8>) {
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("a buffered body")
        .to_vec();
    (status, content_type, body)
}

/// Every row is the status, content type and body the pinned Loki 3.5.1
/// image sent for the same error on the same surface. Several are a 500 for a
/// client error. That is upstream's answer, and a Grafana datasource shows it.
#[tokio::test]
pub(crate) async fn every_tenant_error_surface_answers_as_loki_does() {
    const TEXT: &str = "text/plain; charset=utf-8";
    let slash = TenantRequestError::Resolve(TenantResolveError::Invalid(
        TenantIdError::UnsupportedCharacter {
            tenant: "a/b".into(),
            character: '/',
        },
    ));
    let slash_text = "tenant ID 'a/b' contains unsupported character '/'";
    let missing = TenantRequestError::Resolve(TenantResolveError::Missing);
    let multiple = TenantRequestError::MultipleOrgIds;
    let mut otlp_status = vec![0x12, 50];
    otlp_status.extend_from_slice(slash_text.as_bytes());
    let ruler = |message: &str| {
        format!(
            r#"{{"status":"error","data":null,"errorType":"server_error","error":"{message}"}}"#
        )
        .into_bytes()
    };

    let cases = [
        (
            &missing,
            TenantErrorSurface::Push,
            StatusCode::UNAUTHORIZED,
            TEXT,
            b"no org id\n".to_vec(),
        ),
        (
            &missing,
            TenantErrorSurface::Read,
            StatusCode::UNAUTHORIZED,
            TEXT,
            b"no org id\n".to_vec(),
        ),
        (
            &missing,
            TenantErrorSurface::Ruler,
            StatusCode::UNAUTHORIZED,
            TEXT,
            b"no org id\n".to_vec(),
        ),
        (
            &slash,
            TenantErrorSurface::Push,
            StatusCode::BAD_REQUEST,
            TEXT,
            format!("{slash_text}\n").into_bytes(),
        ),
        (
            &multiple,
            TenantErrorSurface::Push,
            StatusCode::BAD_REQUEST,
            TEXT,
            b"multiple org IDs present\n".to_vec(),
        ),
        (
            &slash,
            TenantErrorSurface::OtlpPush,
            StatusCode::BAD_REQUEST,
            "application/octet-stream",
            otlp_status,
        ),
        (
            &slash,
            TenantErrorSurface::Read,
            StatusCode::BAD_REQUEST,
            TEXT,
            slash_text.as_bytes().to_vec(),
        ),
        (
            &multiple,
            TenantErrorSurface::Read,
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT,
            b"multiple org IDs present".to_vec(),
        ),
        (
            &slash,
            TenantErrorSurface::QuerierRead,
            StatusCode::BAD_REQUEST,
            TEXT,
            format!("rpc error: code = Code(400) desc = {slash_text}").into_bytes(),
        ),
        (
            &multiple,
            TenantErrorSurface::QuerierRead,
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT,
            b"multiple org IDs present".to_vec(),
        ),
        (
            &slash,
            TenantErrorSurface::InstantLogQuery,
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT,
            slash_text.as_bytes().to_vec(),
        ),
        (
            &slash,
            TenantErrorSurface::Patterns,
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT,
            slash_text.as_bytes().to_vec(),
        ),
        (
            &multiple,
            TenantErrorSurface::Patterns,
            StatusCode::NOT_FOUND,
            TEXT,
            Vec::new(),
        ),
        (
            &slash,
            TenantErrorSurface::Tail,
            StatusCode::BAD_REQUEST,
            TEXT,
            slash_text.as_bytes().to_vec(),
        ),
        (
            &multiple,
            TenantErrorSurface::Tail,
            StatusCode::BAD_REQUEST,
            TEXT,
            b"multiple org IDs present".to_vec(),
        ),
        (
            &slash,
            TenantErrorSurface::Ruler,
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT,
            ruler("no org id"),
        ),
        (
            &multiple,
            TenantErrorSurface::Ruler,
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT,
            ruler("no org id"),
        ),
        (
            &slash,
            TenantErrorSurface::PrometheusRuler,
            StatusCode::INTERNAL_SERVER_ERROR,
            TEXT,
            ruler("no valid org id found"),
        ),
    ];

    for (error, surface, status, content_type, body) in cases {
        let expected = (status, content_type.to_string(), body);
        check!(
            parts_for_test(tenant_error_response(error, surface)).await == expected,
            "{surface:?} {error}"
        );
    }
}
