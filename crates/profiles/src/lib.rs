//! Krabka profiles ingest service.
//!
//! The distributor serves the push.v1, `/ingest`, and OTLP `v1development`
//! profiles doors and writes to a WAL partitioned by
//! `(tenant, series_fingerprint)`. The block-builder consumer group builds the
//! samples fact table, the deduped per-block `SymbolDb`, and the
//! `ProfileIndex`. The compactor merges those blocks and deletes the ones
//! nothing needs any more; see [`lifecycle`].
#![forbid(unsafe_code)]

pub mod all;
pub mod blockbuilder;
pub mod cold_store;
pub mod compactor;
pub mod distributor;
pub mod error;
pub mod hot_store;
pub mod ids;
pub mod ingest;
pub mod lifecycle;
pub mod limits;
pub mod metrics;
pub mod query;
pub mod query_frontend;
pub mod recording;
mod server_task;
pub mod symbolizer;
mod tenant_from_headers;
#[cfg(test)]
mod test_support;
pub mod wal;
pub mod wire;

pub use blockbuilder::{BuiltSample, build_block, intern_record, object_key, run, samples_batch};
pub use error::ProfilesError;
pub use ids::{
    DefaultMs, EndMs, ExternalPartition, LocalPartition, MaxValue, MinValue, NowMs, StartMs,
};
pub use limits::{LimitError, Limits, OverridesError, OverridesProvider};
pub use server_task::ServerTask;
pub use wal::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample,
    WalSymbolSet, partition_key,
};

use self::tenant_from_headers::tenant_from_headers;

#[cfg(test)]
mod tests {
    use assert2::check;
    use axum::http::{HeaderMap, HeaderValue};
    use krabka_blockstore::{
        TENANT_HEADER, TenantId, TenantIdError, TenantPolicy, TenantResolveError,
    };
    use krabka_observability::server_security::TenantDenied;

    use super::*;

    struct StatusCodeCase {
        error: ProfilesError,
        expected: u16,
    }

    struct TenantResolutionCase {
        name: &'static str,
        headers: HeaderMap,
        anonymous: Result<TenantId, TenantResolveError>,
        fallback: Result<TenantId, TenantResolveError>,
        required: Result<TenantId, TenantResolveError>,
    }

    #[test]
    fn status_codes_map() {
        for case in [
            StatusCodeCase {
                error: ProfilesError::UnsupportedFormat("x".into()),
                expected: 415,
            },
            StatusCodeCase {
                error: ProfilesError::Decode("x".into()),
                expected: 400,
            },
            StatusCodeCase {
                error: ProfilesError::Unprocessable("x".into()),
                expected: 422,
            },
            StatusCodeCase {
                error: ProfilesError::Tenant(TenantResolveError::Missing),
                expected: 400,
            },
            StatusCodeCase {
                error: ProfilesError::TenantDenied(TenantDenied {
                    principal: "grafana".into(),
                    tenant: TenantId::new("tenant-b").unwrap(),
                }),
                expected: 403,
            },
            StatusCodeCase {
                error: ProfilesError::from(LimitError::MaxSeries {
                    limit: 1,
                    observed: 2,
                }),
                expected: 429,
            },
        ] {
            check!(case.error.status_code() == case.expected);
        }
    }

    // The resolver hands the raw header bytes to `TenantId::resolve`, so the
    // policy it is given settles a request without a tenant, and a malformed
    // value is an error under every policy. A non-UTF-8 value in particular
    // is an error and never a request without a tenant.
    #[test]
    fn the_resolver_reads_the_header_bytes_under_the_policy_it_is_given() {
        let header = |value: &[u8]| {
            let mut headers = HeaderMap::new();
            headers.insert(TENANT_HEADER, HeaderValue::from_bytes(value).unwrap());
            headers
        };
        let named = |name: &str| Ok(TenantId::new(name).unwrap());
        let unsupported = |tenant: &str, character| {
            Err(TenantResolveError::Invalid(
                TenantIdError::UnsupportedCharacter {
                    tenant: tenant.into(),
                    character,
                },
            ))
        };
        let single = TenantPolicy::Fallback(TenantId::new("single").unwrap());
        let cases = [
            TenantResolutionCase {
                name: "absent",
                headers: HeaderMap::new(),
                anonymous: named("anonymous"),
                fallback: named("single"),
                required: Err(TenantResolveError::Missing),
            },
            TenantResolutionCase {
                name: "empty",
                headers: header(b""),
                anonymous: named("anonymous"),
                fallback: named("single"),
                required: Err(TenantResolveError::Missing),
            },
            TenantResolutionCase {
                name: "named",
                headers: header(b"tenant-a"),
                anonymous: named("tenant-a"),
                fallback: named("tenant-a"),
                required: named("tenant-a"),
            },
            TenantResolutionCase {
                name: "separator",
                headers: header(b"a/b"),
                anonymous: unsupported("a/b", '/'),
                fallback: unsupported("a/b", '/'),
                required: unsupported("a/b", '/'),
            },
            TenantResolutionCase {
                name: "not UTF-8",
                headers: header(b"a\xff"),
                // dskit names the raw byte `0xFF` as a code point, which is `ÿ`.
                anonymous: unsupported("a\u{fffd}", '\u{ff}'),
                fallback: unsupported("a\u{fffd}", '\u{ff}'),
                required: unsupported("a\u{fffd}", '\u{ff}'),
            },
        ];

        for case in cases {
            check!(
                tenant_from_headers(&case.headers, &TenantPolicy::anonymous()) == case.anonymous,
                "{}",
                case.name
            );
            check!(
                tenant_from_headers(&case.headers, &single) == case.fallback,
                "{}",
                case.name
            );
            check!(
                tenant_from_headers(&case.headers, &TenantPolicy::Required) == case.required,
                "{}",
                case.name
            );
        }
    }
}
