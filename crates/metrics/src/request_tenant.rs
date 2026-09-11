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
    type Case<'a> = (
        &'a str,
        Option<&'a [u8]>,
        Result<TenantId, RequestTenantError>,
    );

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
            ("named", Some(b"tenant-a"), Ok(tenant("tenant-a"))),
            ("absent", None, Err(MISSING)),
            ("empty", Some(b""), Err(MISSING)),
            ("separator", Some(b"a/b"), Err(unsupported("a/b", '/'))),
            ("space", Some(b"a b"), Err(unsupported("a b", ' '))),
            (
                "too long",
                Some(too_long.as_bytes()),
                Err(invalid(TenantIdError::TooLong)),
            ),
            (
                "too long and unsupported",
                Some(too_long_and_bad.as_bytes()),
                Err(unsupported(&too_long_and_bad, '/')),
            ),
            (
                "dot dot",
                Some(b".."),
                Err(invalid(TenantIdError::RelativePathSegment)),
            ),
            ("two tenants", Some(b"a|b"), too_many(2)),
            ("three tenants", Some(b"a|b|c"), too_many(3)),
            ("one tenant twice", Some(b"a|a"), Ok(tenant("a"))),
            ("a repeat among two", Some(b"a|a|b"), too_many(2)),
            ("repeats out of order", Some(b"b|a|b"), too_many(2)),
            ("a trailing empty part", Some(b"a|"), too_many(2)),
            ("a leading empty part", Some(b"|a"), too_many(2)),
            ("a repeat and an empty part", Some(b"a|a|"), too_many(2)),
            (
                "an invalid second part",
                Some(b"a|b/c"),
                Err(unsupported("b/c", '/')),
            ),
            (
                "an invalid first part",
                Some(b"a/b|c"),
                Err(unsupported("a/b", '/')),
            ),
            (
                "a dot part",
                Some(b".|a"),
                Err(invalid(TenantIdError::RelativePathSegment)),
            ),
            (
                "a dot dot part",
                Some(b"a|.."),
                Err(invalid(TenantIdError::RelativePathSegment)),
            ),
            // Mimir answers these two with a 500 on a query and accepts a push
            // under an empty tenant. Krabka cannot hold an empty tenant.
            ("only a separator", Some(b"|"), Err(MISSING)),
            ("only separators", Some(b"||"), Err(MISSING)),
        ];

        for (name, value, expected) in cases {
            check!(resolve_request_tenant(value) == expected, "{name}");
        }
    }

    /// Mimir answers each error before its API handler runs, through Go's
    /// `http.Error`, so a client gets plain text and a trailing line break
    /// and never a JSON envelope. Grafana shows that body to its user.
    #[tokio::test]
    async fn an_error_response_is_mimirs_status_and_plain_text_body() {
        let cases = [
            (MISSING, StatusCode::UNAUTHORIZED, "no org id\n"),
            (
                unsupported("a/b", '/'),
                StatusCode::UNAUTHORIZED,
                "tenant ID 'a/b' contains unsupported character '/'\n",
            ),
            (
                invalid(TenantIdError::TooLong),
                StatusCode::UNAUTHORIZED,
                "tenant ID is too long: max 150 characters\n",
            ),
            (
                invalid(TenantIdError::RelativePathSegment),
                StatusCode::UNAUTHORIZED,
                "tenant ID is '.' or '..'\n",
            ),
            (
                RequestTenantError::TooManyTenants { actual: 3 },
                StatusCode::UNPROCESSABLE_ENTITY,
                "too many tenant IDs present in the request. max: 1 actual: 3\n",
            ),
        ];

        for (error, status, body) in cases {
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

mod authorized_tenant_from_headers;
mod max_request_tenants;
mod request_tenant_error;
mod resolve_request_tenant;
mod tenant_access_error;
mod tenant_from_headers;
mod tenant_from_metadata;
mod tenant_separator;

pub use authorized_tenant_from_headers::authorized_tenant_from_headers;
pub use max_request_tenants::MAX_REQUEST_TENANTS;
pub use request_tenant_error::RequestTenantError;
pub use resolve_request_tenant::resolve_request_tenant;
pub use tenant_access_error::TenantAccessError;
pub use tenant_from_headers::tenant_from_headers;
pub use tenant_from_metadata::tenant_from_metadata;
use tenant_separator::TENANT_SEPARATOR;
