use axum::body::to_bytes;

use crate::tests::prelude::{
    AllowAllIngestLimiter, IntoResponse, Response, StatusCode, TenantErrorSurface, TenantId,
    TenantIdError, TenantRequestError, TenantResolveError, WalLogRecord, check, check_ingest_quota,
    grpc_tenant, resolve_federated_tenants, resolve_single_tenant, tenant_error_response,
};
use crate::{
    DistributorError, IngestLimitError, LogIngestLimiter, async_trait, server_security::Principal,
};

mod a_batch_with_a_record_for_another_tenant_never_reaches_the_limiter {
use super::*;
use crate::tests::api_record_for_test;

struct PanickingIngestLimiter;

#[async_trait]
impl LogIngestLimiter for PanickingIngestLimiter {
    async fn check(
        &self,
        _principal: &Principal,
        _tenant: &TenantId,
        _records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        panic!("a batch under two tenants reached the limiter");
    }
}

/// The limiter checks one tenant per call. A record under a second tenant in
/// the batch would be admitted by the first tenant's ACLs and quota, so the
/// batch is refused before the limiter is asked, and no record is checked
/// against the wrong tenant.
#[tokio::test]
pub(crate) async fn a_batch_with_a_record_for_another_tenant_never_reaches_the_limiter() {
    let tenant = TenantId::new("tenant-a").expect("a valid tenant id");
    let mixed = [
        api_record_for_test("tenant-a"),
        api_record_for_test("tenant-b"),
    ];

    let refused = check_ingest_quota(
        &PanickingIngestLimiter,
        &Principal::Unauthenticated,
        &tenant,
        &mixed,
    )
    .await;
    check!(
        matches!(
            &refused,
            Err(DistributorError::RecordTenantMismatch { tenant, record_tenant })
                if tenant == "tenant-a" && record_tenant == "tenant-b"
        ),
        "{refused:?}"
    );
    check!(
        refused.map_err(|error| error.into_response().status())
            == Err(StatusCode::INTERNAL_SERVER_ERROR)
    );

    let single = [
        api_record_for_test("tenant-a"),
        api_record_for_test("tenant-a"),
    ];
    check!(
        check_ingest_quota(
            &AllowAllIngestLimiter,
            &Principal::Unauthenticated,
            &tenant,
            &single
        )
        .await
        .is_ok()
    );
}
}
mod every_tenant_error_surface_answers_as_loki_does;

struct RowForTest {
    name: &'static str,
    value: Option<&'static [u8]>,
    expected: Result<Vec<TenantId>, TenantRequestError>,
}

/// Every row is a header value the pinned Loki 3.5.1 image, with
/// `querier.multi_tenant_queries_enabled` on, was sent on a range query. A
/// malformed part refuses the whole query, and an empty part names no tenant.
#[test]
pub(crate) fn a_federated_header_resolves_every_part_sorted_and_without_repeats() {
    let tenants = |names: &[&str]| {
        Ok(names
            .iter()
            .map(|name| TenantId::new(*name).expect("a valid tenant id"))
            .collect::<Vec<_>>())
    };
    let unsupported = |tenant: &str, character| {
        Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
            TenantIdError::UnsupportedCharacter {
                tenant: tenant.into(),
                character,
            },
        )))
    };
    let missing = Err(TenantRequestError::Resolve(TenantResolveError::Missing));

    let cases: [RowForTest; 11] = [
        RowForTest {
            name: "one tenant",
            value: Some(b"tenant-a"),
            expected: tenants(&["tenant-a"]),
        },
        RowForTest {
            name: "absent",
            value: None,
            expected: missing.clone(),
        },
        RowForTest {
            name: "empty",
            value: Some(b""),
            expected: missing.clone(),
        },
        RowForTest {
            name: "two tenants",
            value: Some(b"b|a"),
            expected: tenants(&["a", "b"]),
        },
        RowForTest {
            name: "a repeat",
            value: Some(b"b|a|b"),
            expected: tenants(&["a", "b"]),
        },
        RowForTest {
            name: "an empty middle part",
            value: Some(b"a||b"),
            expected: tenants(&["a", "b"]),
        },
        RowForTest {
            name: "a trailing empty part",
            value: Some(b"a|"),
            expected: tenants(&["a"]),
        },
        RowForTest {
            name: "an invalid part",
            value: Some(b"a|b/c"),
            expected: unsupported("b/c", '/'),
        },
        RowForTest {
            name: "a padded part",
            value: Some(b"a | b"),
            expected: unsupported("a ", ' '),
        },
        RowForTest {
            name: "a dot dot part",
            value: Some(b"a|.."),
            expected: Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
                TenantIdError::RelativePathSegment,
            ))),
        },
        RowForTest {
            name: "only a separator",
            value: Some(b"|"),
            expected: missing,
        },
    ];

    for case in cases {
        check!(
            resolve_federated_tenants(case.value) == case.expected,
            "{}",
            case.name
        );
    }
}

struct GrpcTenantCase {
    name: &'static str,
    value: Option<&'static str>,
    code: tonic::Code,
    message: &'static str,
}

/// Loki has no OTLP gRPC door, so these codes follow the HTTP push: 401
/// becomes `UNAUTHENTICATED` and 400 becomes `INVALID_ARGUMENT`, with the
/// same message.
#[test]
pub(crate) fn a_grpc_export_without_one_valid_tenant_gets_a_status_code() {
    let metadata = |value: Option<&str>| {
        let mut metadata = tonic::metadata::MetadataMap::new();
        if let Some(value) = value {
            metadata.insert("x-scope-orgid", value.parse().expect("a metadata value"));
        }
        metadata
    };
    let refused = [
        GrpcTenantCase {
            name: "absent",
            value: None,
            code: tonic::Code::Unauthenticated,
            message: "no org id",
        },
        GrpcTenantCase {
            name: "empty",
            value: Some(""),
            code: tonic::Code::Unauthenticated,
            message: "no org id",
        },
        GrpcTenantCase {
            name: "separator",
            value: Some("a/b"),
            code: tonic::Code::InvalidArgument,
            message: "tenant ID 'a/b' contains unsupported character '/'",
        },
        GrpcTenantCase {
            name: "two tenants",
            value: Some("a|b"),
            code: tonic::Code::InvalidArgument,
            message: "multiple org IDs present",
        },
    ];

    for case in refused {
        let status = grpc_tenant(&metadata(case.value)).expect_err("the export is refused");
        check!(
            status.code() == case.code && status.message() == case.message,
            "{}",
            case.name
        );
    }
    check!(
        grpc_tenant(&metadata(Some("tenant-a"))).ok()
            == Some(TenantId::new("tenant-a").expect("a valid tenant id"))
    );
}

struct SingleTenantRowForTest<'a> {
    name: &'a str,
    value: Option<&'a [u8]>,
    expected: Result<TenantId, TenantRequestError>,
}

/// Every row is a header value the pinned Loki 3.5.1 image was sent on a
/// push, and the tenant or the error it answered with. `dskit` checks every
/// `|` part before it counts the distinct parts, so a malformed part wins over
/// a count, a repeated part counts once, and an empty part counts but is not
/// checked.
#[test]
pub(crate) fn a_single_tenant_header_resolves_as_dskit_resolves_it() {
    let tenant = |name: &str| Ok(TenantId::new(name).expect("a valid tenant id"));
    let unsupported = |tenant: &str, character| {
        Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
            TenantIdError::UnsupportedCharacter {
                tenant: tenant.into(),
                character,
            },
        )))
    };
    let invalid = |error| {
        Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
            error,
        )))
    };
    let missing = Err(TenantRequestError::Resolve(TenantResolveError::Missing));
    let multiple = Err(TenantRequestError::MultipleOrgIds);
    let too_long = "x".repeat(151);

    let cases: [SingleTenantRowForTest<'_>; 15] = [
        SingleTenantRowForTest {
            name: "named",
            value: Some(b"tenant-a"),
            expected: tenant("tenant-a"),
        },
        SingleTenantRowForTest {
            name: "absent",
            value: None,
            expected: missing.clone(),
        },
        SingleTenantRowForTest {
            name: "empty",
            value: Some(b""),
            expected: missing.clone(),
        },
        SingleTenantRowForTest {
            name: "separator",
            value: Some(b"a/b"),
            expected: unsupported("a/b", '/'),
        },
        SingleTenantRowForTest {
            name: "dot dot",
            value: Some(b".."),
            expected: invalid(TenantIdError::RelativePathSegment),
        },
        SingleTenantRowForTest {
            name: "two tenants",
            value: Some(b"a|b"),
            expected: multiple.clone(),
        },
        SingleTenantRowForTest {
            name: "an invalid second part",
            value: Some(b"a|b/c"),
            expected: unsupported("b/c", '/'),
        },
        SingleTenantRowForTest {
            name: "an empty middle part",
            value: Some(b"a||b"),
            expected: multiple.clone(),
        },
        SingleTenantRowForTest {
            name: "a padded part",
            value: Some(b"a | b"),
            expected: unsupported("a ", ' '),
        },
        SingleTenantRowForTest {
            name: "one tenant twice",
            value: Some(b"a|a"),
            expected: tenant("a"),
        },
        SingleTenantRowForTest {
            name: "a trailing empty part",
            value: Some(b"a|"),
            expected: multiple.clone(),
        },
        SingleTenantRowForTest {
            name: "a leading empty part",
            value: Some(b"|a"),
            expected: multiple,
        },
        // Upstream serves these as the empty tenant. Krabka cannot hold one.
        SingleTenantRowForTest {
            name: "only a separator",
            value: Some(b"|"),
            expected: missing.clone(),
        },
        SingleTenantRowForTest {
            name: "only separators",
            value: Some(b"||"),
            expected: missing,
        },
        SingleTenantRowForTest {
            name: "too long",
            value: Some(too_long.as_bytes()),
            expected: invalid(TenantIdError::TooLong),
        },
    ];

    for row in cases {
        check!(
            resolve_single_tenant(row.value) == row.expected,
            "{}",
            row.name
        );
    }
}
