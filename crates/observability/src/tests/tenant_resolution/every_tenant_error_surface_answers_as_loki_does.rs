use super::*;

#[derive(Debug, Eq, PartialEq)]
struct TenantResponseParts {
    status: StatusCode,
    content_type: String,
    body: Vec<u8>,
}

async fn parts_for_test(response: Response) -> TenantResponseParts {
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
    TenantResponseParts {
        status,
        content_type,
        body,
    }
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

    struct TenantErrorCase<'a> {
        error: &'a TenantRequestError,
        surface: TenantErrorSurface,
        status: StatusCode,
        content_type: &'static str,
        body: Vec<u8>,
    }
    let cases = [
        TenantErrorCase {
            error: &missing,
            surface: TenantErrorSurface::Push,
            status: StatusCode::UNAUTHORIZED,
            content_type: TEXT,
            body: b"no org id\n".to_vec(),
        },
        TenantErrorCase {
            error: &missing,
            surface: TenantErrorSurface::Read,
            status: StatusCode::UNAUTHORIZED,
            content_type: TEXT,
            body: b"no org id\n".to_vec(),
        },
        TenantErrorCase {
            error: &missing,
            surface: TenantErrorSurface::Ruler,
            status: StatusCode::UNAUTHORIZED,
            content_type: TEXT,
            body: b"no org id\n".to_vec(),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::Push,
            status: StatusCode::BAD_REQUEST,
            content_type: TEXT,
            body: format!("{slash_text}\n").into_bytes(),
        },
        TenantErrorCase {
            error: &multiple,
            surface: TenantErrorSurface::Push,
            status: StatusCode::BAD_REQUEST,
            content_type: TEXT,
            body: b"multiple org IDs present\n".to_vec(),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::OtlpPush,
            status: StatusCode::BAD_REQUEST,
            content_type: "application/octet-stream",
            body: otlp_status,
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::Read,
            status: StatusCode::BAD_REQUEST,
            content_type: TEXT,
            body: slash_text.as_bytes().to_vec(),
        },
        TenantErrorCase {
            error: &multiple,
            surface: TenantErrorSurface::Read,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: TEXT,
            body: b"multiple org IDs present".to_vec(),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::QuerierRead,
            status: StatusCode::BAD_REQUEST,
            content_type: TEXT,
            body: format!("rpc error: code = Code(400) desc = {slash_text}").into_bytes(),
        },
        TenantErrorCase {
            error: &multiple,
            surface: TenantErrorSurface::QuerierRead,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: TEXT,
            body: b"multiple org IDs present".to_vec(),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::InstantLogQuery,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: TEXT,
            body: slash_text.as_bytes().to_vec(),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::Patterns,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: TEXT,
            body: slash_text.as_bytes().to_vec(),
        },
        TenantErrorCase {
            error: &multiple,
            surface: TenantErrorSurface::Patterns,
            status: StatusCode::NOT_FOUND,
            content_type: TEXT,
            body: Vec::new(),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::Tail,
            status: StatusCode::BAD_REQUEST,
            content_type: TEXT,
            body: slash_text.as_bytes().to_vec(),
        },
        TenantErrorCase {
            error: &multiple,
            surface: TenantErrorSurface::Tail,
            status: StatusCode::BAD_REQUEST,
            content_type: TEXT,
            body: b"multiple org IDs present".to_vec(),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::Ruler,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: TEXT,
            body: ruler("no org id"),
        },
        TenantErrorCase {
            error: &multiple,
            surface: TenantErrorSurface::Ruler,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: TEXT,
            body: ruler("no org id"),
        },
        TenantErrorCase {
            error: &slash,
            surface: TenantErrorSurface::PrometheusRuler,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: TEXT,
            body: ruler("no valid org id found"),
        },
    ];

    for case in cases {
        let expected = TenantResponseParts {
            status: case.status,
            content_type: case.content_type.to_string(),
            body: case.body,
        };
        check!(
            parts_for_test(tenant_error_response(case.error, case.surface)).await == expected,
            "{:?} {}",
            case.surface,
            case.error
        );
    }
}
