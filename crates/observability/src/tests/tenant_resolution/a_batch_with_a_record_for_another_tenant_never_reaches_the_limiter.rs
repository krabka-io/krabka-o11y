use super::*;
use crate::tests::api_record_for_test::api_record_for_test;

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
