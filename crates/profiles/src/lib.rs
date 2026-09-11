//! Krabka profiles ingest service.
//!
//! The distributor serves the push.v1, `/ingest`, and OTLP `v1development`
//! profiles doors and writes to a WAL partitioned by
//! `(tenant, series_fingerprint)`. The block-builder consumer group builds the
//! samples fact table, the deduped per-block `SymbolDb`, and the
//! `ProfileIndex`.
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
pub mod limits;
pub mod metrics;
pub mod query;
pub mod query_frontend;
pub mod symbolizer;
mod tenant_from_headers;
pub mod wal;
pub mod wire;

pub use blockbuilder::{BuiltSample, build_block, intern_record, object_key, run, samples_batch};
pub use error::ProfilesError;
pub use ids::{
    DefaultMs, EndMs, ExternalPartition, IngestBytes, IngestItems, LocalPartition, MaxValue,
    MinValue, NowMs, StartMs,
};
pub use limits::{LimitError, Limits, OverridesError, OverridesProvider};
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

    #[test]
    fn status_codes_map() {
        for (err, want) in [
            (ProfilesError::UnsupportedFormat("x".into()), 415),
            (ProfilesError::Decode("x".into()), 400),
            (ProfilesError::Tenant(TenantResolveError::Missing), 400),
            (
                ProfilesError::TenantDenied(TenantDenied {
                    principal: "grafana".into(),
                    tenant: TenantId::new("tenant-b").unwrap(),
                }),
                403,
            ),
            (
                ProfilesError::from(LimitError::MaxSeries {
                    limit: 1,
                    observed: 2,
                }),
                429,
            ),
        ] {
            check!(err.status_code() == want);
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
            (
                "absent",
                HeaderMap::new(),
                named("anonymous"),
                named("single"),
                Err(TenantResolveError::Missing),
            ),
            (
                "empty",
                header(b""),
                named("anonymous"),
                named("single"),
                Err(TenantResolveError::Missing),
            ),
            (
                "named",
                header(b"tenant-a"),
                named("tenant-a"),
                named("tenant-a"),
                named("tenant-a"),
            ),
            (
                "separator",
                header(b"a/b"),
                unsupported("a/b", '/'),
                unsupported("a/b", '/'),
                unsupported("a/b", '/'),
            ),
            (
                "not UTF-8",
                header(b"a\xff"),
                // dskit names the raw byte `0xFF` as a code point, which is `ÿ`.
                unsupported("a\u{fffd}", '\u{ff}'),
                unsupported("a\u{fffd}", '\u{ff}'),
                unsupported("a\u{fffd}", '\u{ff}'),
            ),
        ];

        for (name, headers, anonymous, fallen_back, required) in cases {
            check!(
                tenant_from_headers(&headers, &TenantPolicy::anonymous()) == anonymous,
                "{name}"
            );
            check!(
                tenant_from_headers(&headers, &single) == fallen_back,
                "{name}"
            );
            check!(
                tenant_from_headers(&headers, &TenantPolicy::Required) == required,
                "{name}"
            );
        }
    }
}
