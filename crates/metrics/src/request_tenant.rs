//! The tenant of a metrics request, resolved the same way at every boundary.
//!
//! The distributor HTTP push, the distributor OTLP gRPC export, and every
//! `PromQL` read in `krabka-promql` get their tenant from
//! [`resolve_request_tenant`]. That function adds only Grafana Mimir's `|`
//! tenant list to [`TenantId::resolve`], so no metrics path has a tenant rule
//! of its own.
//!
//! An HTTP boundary calls [`authorized_tenant_from_headers`], which also checks
//! that the request's principal may use the tenant. The check passes every
//! tenant when the server has no credentials file.

use std::collections::BTreeSet;

use axum::{
    http::{HeaderMap, HeaderValue, StatusCode, header::X_CONTENT_TYPE_OPTIONS},
    response::{IntoResponse, Response},
};
use krabka_blockstore::{TENANT_HEADER, TenantId, TenantPolicy, TenantResolveError};
use krabka_observability::server_security::{Principal, TenantDenied, authorize_tenant};
use thiserror::Error;
use tonic::metadata::{MetadataMap, MetadataValue};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use axum::{
        body::to_bytes,
        http::{HeaderMap, HeaderValue, StatusCode, header},
        response::IntoResponse as _,
    };
    use krabka_blockstore::{TenantId, TenantIdError, TenantResolveError};
    use tonic::metadata::MetadataMap;

    use super::{
        MAX_REQUEST_TENANTS, RequestTenantError, resolve_request_tenant, tenant_from_headers,
        tenant_from_metadata,
    };

    struct ErrorResponseCase {
        error: RequestTenantError,
        status: StatusCode,
        body: &'static str,
    }

    fn unsupported(tenant: &str, character: char) -> RequestTenantError {
        RequestTenantError::Resolve(TenantResolveError::Invalid(
            TenantIdError::UnsupportedCharacter {
                tenant: tenant.into(),
                character,
            },
        ))
    }

    fn invalid(error: TenantIdError) -> RequestTenantError {
        RequestTenantError::Resolve(TenantResolveError::Invalid(error))
    }

    fn tenant(name: &str) -> TenantId {
        TenantId::new(name).expect("a valid tenant id")
    }

    // A row of the oracle table: its name, the header value, and the answer.
    struct Case<'a> {
        name: &'a str,
        value: Option<&'a [u8]>,
        expected: Result<TenantId, RequestTenantError>,
    }

    fn case<'a>(
        name: &'a str,
        value: Option<&'a [u8]>,
        expected: Result<TenantId, RequestTenantError>,
    ) -> Case<'a> {
        Case {
            name,
            value,
            expected,
        }
    }

    const MISSING: RequestTenantError = RequestTenantError::Resolve(TenantResolveError::Missing);

    /// Every row is a header value that the pinned Mimir 2.16.1 image was
    /// sent, and the answer it gave. Mimir checks every `|` part in header
    /// order before it counts the distinct parts, so an invalid part wins over
    /// a count, and a repeated part counts once.
    #[test]
    fn every_header_value_resolves_as_mimir_resolves_it() {
        let too_long = "x".repeat(151);
        let too_long_and_bad = format!("{too_long}/");
        let too_many = |actual| Err(RequestTenantError::TooManyTenants { actual });
        let cases: [Case<'_>; 22] = [
            case("named", Some(b"tenant-a"), Ok(tenant("tenant-a"))),
            case("absent", None, Err(MISSING)),
            case("empty", Some(b""), Err(MISSING)),
            case("separator", Some(b"a/b"), Err(unsupported("a/b", '/'))),
            case("space", Some(b"a b"), Err(unsupported("a b", ' '))),
            case(
                "too long",
                Some(too_long.as_bytes()),
                Err(invalid(TenantIdError::TooLong)),
            ),
            case(
                "too long and unsupported",
                Some(too_long_and_bad.as_bytes()),
                Err(unsupported(&too_long_and_bad, '/')),
            ),
            case(
                "dot dot",
                Some(b".."),
                Err(invalid(TenantIdError::RelativePathSegment)),
            ),
            case("two tenants", Some(b"a|b"), too_many(2)),
            case("three tenants", Some(b"a|b|c"), too_many(3)),
            case("one tenant twice", Some(b"a|a"), Ok(tenant("a"))),
            case("a repeat among two", Some(b"a|a|b"), too_many(2)),
            case("repeats out of order", Some(b"b|a|b"), too_many(2)),
            case("a trailing empty part", Some(b"a|"), too_many(2)),
            case("a leading empty part", Some(b"|a"), too_many(2)),
            case("a repeat and an empty part", Some(b"a|a|"), too_many(2)),
            case(
                "an invalid second part",
                Some(b"a|b/c"),
                Err(unsupported("b/c", '/')),
            ),
            case(
                "an invalid first part",
                Some(b"a/b|c"),
                Err(unsupported("a/b", '/')),
            ),
            case(
                "a dot part",
                Some(b".|a"),
                Err(invalid(TenantIdError::RelativePathSegment)),
            ),
            case(
                "a dot dot part",
                Some(b"a|.."),
                Err(invalid(TenantIdError::RelativePathSegment)),
            ),
            // Mimir answers these two with a 500 on a query and accepts a push
            // under an empty tenant. Krabka cannot hold an empty tenant.
            case("only a separator", Some(b"|"), Err(MISSING)),
            case("only separators", Some(b"||"), Err(MISSING)),
        ];

        for test_case in cases {
            check!(
                resolve_request_tenant(test_case.value) == test_case.expected,
                "{}",
                test_case.name
            );
        }
    }

    /// Mimir answers each error before its API handler runs, through Go's
    /// `http.Error`, so a client gets plain text and a trailing line break
    /// and never a JSON envelope. Grafana shows that body to its user.
    #[tokio::test]
    async fn an_error_response_is_mimirs_status_and_plain_text_body() {
        let cases = [
            ErrorResponseCase {
                error: MISSING,
                status: StatusCode::UNAUTHORIZED,
                body: "no org id\n",
            },
            ErrorResponseCase {
                error: unsupported("a/b", '/'),
                status: StatusCode::UNAUTHORIZED,
                body: "tenant ID 'a/b' contains unsupported character '/'\n",
            },
            ErrorResponseCase {
                error: invalid(TenantIdError::TooLong),
                status: StatusCode::UNAUTHORIZED,
                body: "tenant ID is too long: max 150 characters\n",
            },
            ErrorResponseCase {
                error: invalid(TenantIdError::RelativePathSegment),
                status: StatusCode::UNAUTHORIZED,
                body: "tenant ID is '.' or '..'\n",
            },
            ErrorResponseCase {
                error: RequestTenantError::TooManyTenants { actual: 3 },
                status: StatusCode::UNPROCESSABLE_ENTITY,
                body: "too many tenant IDs present in the request. max: 1 actual: 3\n",
            },
        ];

        for ErrorResponseCase {
            error,
            status,
            body,
        } in cases
        {
            let response = error.into_response();
            check!(response.status() == status, "{body}");
            check!(
                response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .map(HeaderValue::as_bytes)
                    == Some(&b"text/plain; charset=utf-8"[..]),
                "{body}"
            );
            check!(
                response
                    .headers()
                    .get(header::X_CONTENT_TYPE_OPTIONS)
                    .map(HeaderValue::as_bytes)
                    == Some(&b"nosniff"[..]),
                "{body}"
            );
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("a string body reads back");
            check!(bytes.as_ref() == body.as_bytes());
        }
        check!(MAX_REQUEST_TENANTS == 1);
    }

    /// An HTTP request that repeats the header gets its tenant from the first
    /// value, as the Mimir image does: `b/c` then `a` is rejected, and `a`
    /// then `b/c` is the tenant `a`.
    #[test]
    fn the_first_header_value_names_the_tenant() {
        let headers = |values: &[&'static str]| {
            let mut headers = HeaderMap::new();
            for value in values {
                headers.append("X-Scope-OrgID", value.parse().expect("a header value"));
            }
            headers
        };

        check!(tenant_from_headers(&headers(&["a", "b/c"])) == Ok(tenant("a")));
        check!(tenant_from_headers(&headers(&["b/c", "a"])) == Err(unsupported("b/c", '/')));
        check!(tenant_from_headers(&HeaderMap::new()) == Err(MISSING));
    }

    #[test]
    fn grpc_metadata_resolves_through_the_same_rules() {
        let mut metadata = MetadataMap::new();
        check!(tenant_from_metadata(&metadata) == Err(MISSING));

        metadata.insert("x-scope-orgid", "a|b".parse().expect("a metadata value"));
        check!(
            tenant_from_metadata(&metadata)
                == Err(RequestTenantError::TooManyTenants { actual: 2 })
        );

        metadata.insert(
            "x-scope-orgid",
            "tenant-a".parse().expect("a metadata value"),
        );
        assert!(tenant_from_metadata(&metadata) == Ok(tenant("tenant-a")));
    }
}
mod resolve_request_tenant;
pub use resolve_request_tenant::resolve_request_tenant;

/// The byte that separates tenant ids in one `X-Scope-OrgID` value.
pub(super) const TENANT_SEPARATOR: u8 = b'|';

/// Resolves the tenant of a gRPC request from its `x-scope-orgid` metadata.
///
/// # Errors
///
/// Returns the errors of [`resolve_request_tenant`].
pub fn tenant_from_metadata(metadata: &MetadataMap) -> Result<TenantId, RequestTenantError> {
    resolve_request_tenant(metadata.get(TENANT_HEADER).map(MetadataValue::as_bytes))
}

/// Resolves the tenant of an HTTP request, and checks that `principal` may use it.
///
/// The authentication layer puts `principal` into the request. A server with
/// no credentials file gives `Principal::Unauthenticated`, which may use every
/// tenant, as Grafana Mimir allows by default.
///
/// # Errors
///
/// Returns [`TenantAccessError::Unresolved`] with the errors of
/// [`tenant_from_headers`]. Returns [`TenantAccessError::Denied`] when the
/// principal's grant does not include the tenant. `authorize_tenant` also
/// reports that denial to the server's security events.
pub fn authorized_tenant_from_headers(
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<TenantId, TenantAccessError> {
    let tenant = tenant_from_headers(headers)?;
    authorize_tenant(principal, &tenant)?;
    Ok(tenant)
}

/// Resolves the tenant of an HTTP request from its `X-Scope-OrgID` header.
///
/// When the request repeats the header, the first value names the tenant, as
/// Go's `Header.Get` reads it in Mimir.
///
/// # Errors
///
/// Returns the errors of [`resolve_request_tenant`].
pub fn tenant_from_headers(headers: &HeaderMap) -> Result<TenantId, RequestTenantError> {
    resolve_request_tenant(headers.get(TENANT_HEADER).map(HeaderValue::as_bytes))
}


/// Most distinct tenants one metrics request can name.
///
/// Grafana Mimir reads a `|` in `X-Scope-OrgID` as a list of tenants for a
/// federated query. With tenant federation off, which is its default, it
/// accepts one tenant and reports this number in its rejection.
pub const MAX_REQUEST_TENANTS: usize = 1;

/// Why a metrics request does not resolve to one tenant.
///
/// Each message is the text the pinned Grafana Mimir 2.16.1 image sends. A
/// Grafana datasource shows that text to its user, so Krabka uses the same
/// words.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RequestTenantError {
    /// The request names no tenant, or a tenant that is not a valid tenant id.
    #[error(transparent)]
    Resolve(#[from] TenantResolveError),

    /// The request names more than [`MAX_REQUEST_TENANTS`] distinct tenants.
    #[error(
        "too many tenant IDs present in the request. max: {MAX_REQUEST_TENANTS} actual: {actual}"
    )]
    TooManyTenants {
        /// The number of distinct tenants the request names.
        actual: usize,
    },
}

impl RequestTenantError {
    /// The HTTP status Mimir answers this error with.
    ///
    /// A missing or invalid tenant is `401 Unauthorized`. Too many tenants is
    /// `422 Unprocessable Entity`.
    #[must_use]
    pub const fn http_status(&self) -> StatusCode {
        match self {
            Self::Resolve(_) => StatusCode::UNAUTHORIZED,
            Self::TooManyTenants { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }
}

impl IntoResponse for RequestTenantError {
    // Mimir rejects the tenant before any API handler runs, through Go's
    // `http.Error`. That writes plain text with a trailing line break, so the
    // body is never the JSON error envelope of the Prometheus API.
    fn into_response(self) -> Response {
        let mut response = format!("{self}\n").into_response();
        *response.status_mut() = self.http_status();
        response.headers_mut().insert(
            X_CONTENT_TYPE_OPTIONS,
            "nosniff".parse().expect("static header"),
        );
        response
    }
}

/// Why a metrics request may not use the tenant it names.
///
/// [`authorized_tenant_from_headers`](super::authorized_tenant_from_headers)
/// returns it. Each variant keeps the response of its source error, so an
/// unusable tenant is still Mimir's plain-text rejection, and a refused
/// principal is a 403.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TenantAccessError {
    /// The request names no usable tenant.
    #[error(transparent)]
    Unresolved(#[from] RequestTenantError),

    /// The request's principal is not granted the tenant.
    #[error(transparent)]
    Denied(#[from] TenantDenied),
}

impl IntoResponse for TenantAccessError {
    fn into_response(self) -> Response {
        match self {
            Self::Unresolved(error) => error.into_response(),
            Self::Denied(error) => error.into_response(),
        }
    }
}
