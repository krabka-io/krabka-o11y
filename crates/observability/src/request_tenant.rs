//! The tenant a logs request names, resolved the same way at every boundary.
//!
//! Every push, query, ruler and delete-request handler gets its tenant from
//! this module. The module adds only Grafana Loki's `|` tenant list to
//! [`TenantId::resolve`], so no logs path has a tenant rule of its own. It
//! also holds the responses, because Loki answers the same resolution error
//! with a different status and body on each surface.

use krabka_blockstore::{TENANT_HEADER, TenantId, TenantPolicy, TenantResolveError};

use crate::{BTreeSet, Error, HeaderMap, Response, StatusCode, encode_otlp_status_message};
mod tenant_error_surface;
mod tenant_request_error;
pub(crate) use tenant_error_surface::TenantErrorSurface;
pub(crate) use tenant_request_error::TenantRequestError;

/// The one tenant an OTLP gRPC export names in its `x-scope-orgid` metadata.
///
/// Loki serves OTLP over HTTP only, so no upstream answer exists for this
/// door. A request without a tenant gets `UNAUTHENTICATED`, because Loki
/// answers the HTTP push with 401. A malformed tenant, or more than one, gets
/// `INVALID_ARGUMENT`, because Loki answers the HTTP push with 400. The
/// message is the text the HTTP push sends.
pub(crate) fn grpc_tenant(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<TenantId, tonic::Status> {
    let value = metadata
        .get(TENANT_HEADER)
        .map(tonic::metadata::MetadataValue::as_encoded_bytes);
    resolve_single_tenant(value).map_err(|error| match error {
        TenantRequestError::Resolve(TenantResolveError::Missing) => {
            tonic::Status::unauthenticated(error.to_string())
        }
        TenantRequestError::Resolve(_) | TenantRequestError::MultipleOrgIds => {
            tonic::Status::invalid_argument(error.to_string())
        }
    })
}

/// Refuses a request without an `X-Scope-OrgID` before its handler runs.
///
/// This is `dskit`'s auth middleware. It checks only that the header is
/// present and not empty, so its 401 comes before every parameter error of the
/// handler behind it. A malformed tenant passes, and the handler refuses it in
/// the way that handler refuses errors.
pub(crate) async fn require_org_id(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if tenant_header_value(request.headers()).is_none_or(<[u8]>::is_empty) {
        return tenant_error_response(
            &TenantRequestError::Resolve(TenantResolveError::Missing),
            TenantErrorSurface::Push,
        );
    }
    next.run(request).await
}

/// Resolves every tenant a federated read names, as `dskit`'s
/// `tenant.TenantIDs` does.
///
/// Loki runs one query across several tenants when the header lists them with
/// `|` and `querier.multi_tenant_queries_enabled` is on. Every part is checked
/// before the query runs, so one malformed part refuses the whole query. The
/// tenants come back sorted and without repeats, as `NormalizeTenantIDs`
/// leaves them. An empty part names no tenant and is dropped: Loki 3.5.1 with
/// multi-tenant queries enabled answers `a||b` and `a|` with the same result
/// as `a|b` and `a`.
///
/// # Errors
///
/// Returns [`TenantRequestError::Resolve`] when the header is absent or
/// empty, when every part is empty, or when a part is not a valid tenant id.
pub(crate) fn resolve_federated_tenants(
    value: Option<&[u8]>,
) -> Result<Vec<TenantId>, TenantRequestError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(TenantResolveError::Missing.into());
    };
    let mut tenants = BTreeSet::new();
    for part in value.split(|byte| *byte == b'|') {
        if !part.is_empty() {
            tenants.insert(TenantId::resolve(Some(part), &TenantPolicy::Required)?);
        }
    }
    if tenants.is_empty() {
        return Err(TenantResolveError::Missing.into());
    }
    Ok(tenants.into_iter().collect())
}

/// Resolves the one tenant a request names, as `dskit`'s `tenant.TenantID`
/// does.
///
/// `dskit` splits the header on `|` and checks every part before it counts the
/// parts. A malformed part therefore wins over a count, and a part that
/// repeats counts once. An empty part is not checked, but it counts as a
/// distinct part, so `a|` names two tenants. Loki 3.5.1 answers a push with
/// each of those errors, and a push with `a|a` goes to tenant `a`.
///
/// # Errors
///
/// Returns [`TenantRequestError::Resolve`] when the header is absent or
/// empty, when every part is empty, or when a part is not a valid tenant id.
/// Returns [`TenantRequestError::MultipleOrgIds`] when the valid parts name
/// more than one distinct tenant.
pub(crate) fn resolve_single_tenant(value: Option<&[u8]>) -> Result<TenantId, TenantRequestError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(TenantResolveError::Missing.into());
    };
    let mut parts = BTreeSet::new();
    let mut tenant = None;
    for part in value.split(|byte| *byte == b'|') {
        if !part.is_empty() {
            tenant = Some(TenantId::resolve(Some(part), &TenantPolicy::Required)?);
        }
        parts.insert(part);
    }
    if parts.len() > 1 {
        return Err(TenantRequestError::MultipleOrgIds);
    }
    // Krabka cannot hold an empty tenant, so a header of separators alone
    // names none. Upstream would serve it as the empty tenant.
    tenant.ok_or_else(|| TenantResolveError::Missing.into())
}

/// The raw `X-Scope-OrgID` bytes of a request, or `None` when it has no such
/// header.
///
/// This reads the first header of that name, as Go's `Header.Get` does. It
/// does no validation. [`resolve_single_tenant`] and
/// [`resolve_federated_tenants`] do that.
pub(crate) fn tenant_header_value(headers: &HeaderMap) -> Option<&[u8]> {
    headers
        .get(TENANT_HEADER)
        .map(axum::http::HeaderValue::as_bytes)
}

/// The response Loki 3.5.1 gives a tenant error on `surface`.
///
/// Every row is a status, a content type and a body captured from the pinned
/// Loki image. Several of them are a 500 for what is a client error. That is
/// what upstream sends, so Krabka sends it too.
pub(crate) fn tenant_error_response(
    error: &TenantRequestError,
    surface: TenantErrorSurface,
) -> Response {
    if matches!(
        error,
        TenantRequestError::Resolve(TenantResolveError::Missing)
    ) {
        // `dskit`'s auth middleware, through Go's `http.Error`.
        return plain_text_error(StatusCode::UNAUTHORIZED, &format!("{error}\n"));
    }
    let multiple = matches!(error, TenantRequestError::MultipleOrgIds);
    match surface {
        TenantErrorSurface::Push => {
            plain_text_error(StatusCode::BAD_REQUEST, &format!("{error}\n"))
        }
        TenantErrorSurface::OtlpPush => response_with_content_type(
            StatusCode::BAD_REQUEST,
            "application/octet-stream",
            encode_otlp_status_message(&error.to_string()),
            false,
        ),
        TenantErrorSurface::Read | TenantErrorSurface::QuerierRead if multiple => {
            plain_text_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
        }
        TenantErrorSurface::Read | TenantErrorSurface::Tail => {
            plain_text_error(StatusCode::BAD_REQUEST, &error.to_string())
        }
        TenantErrorSurface::QuerierRead => plain_text_error(
            StatusCode::BAD_REQUEST,
            &format!("rpc error: code = Code(400) desc = {error}"),
        ),
        TenantErrorSurface::Patterns if multiple => plain_text_error(StatusCode::NOT_FOUND, ""),
        TenantErrorSurface::InstantLogQuery | TenantErrorSurface::Patterns => {
            plain_text_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
        }
        TenantErrorSurface::Ruler => ruler_error("no org id"),
        TenantErrorSurface::PrometheusRuler => ruler_error("no valid org id found"),
    }
}

// The ruler writes its JSON error body itself, not through `http.Error`. So the
// body is JSON, the content type is plain text with no line break, and there
// is no `X-Content-Type-Options` header. The pinned Loki image answers every
// ruler route that way for a malformed or multiple tenant, including the
// Prometheus-compatible `/prometheus/api/v1/rules` and `/alerts`.
fn ruler_error(message: &str) -> Response {
    response_with_content_type(
        StatusCode::INTERNAL_SERVER_ERROR,
        "text/plain; charset=utf-8",
        format!(
            r#"{{"status":"error","data":null,"errorType":"server_error","error":"{message}"}}"#
        ),
        false,
    )
}

// Go's `http.Error` sets `X-Content-Type-Options: nosniff` next to the
// plain-text content type, so a browser does not read the body as HTML. Loki
// answers its tenant errors through it, and `krabka-metrics` sends the same
// header for the same reason.
fn plain_text_error(status: StatusCode, body: &str) -> Response {
    response_with_content_type(status, "text/plain; charset=utf-8", body.to_owned(), true)
}

fn response_with_content_type(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<axum::body::Body>,
    nosniff: bool,
) -> Response {
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", content_type);
    if nosniff {
        builder = builder.header("x-content-type-options", "nosniff");
    }
    builder
        .body(body.into())
        .expect("a static tenant error response is valid")
}
