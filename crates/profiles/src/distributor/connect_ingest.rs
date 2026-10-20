use tracing::Instrument as _;

use super::{
    ConnectError, DistributorState, HeaderMap, IngestOutcome, Principal, ProfilesError,
    RequestOutcome, TenantResolveError, authorize_tenant, connect_error, ingest_request_span,
    process_raw, record_ingest_outcome, tenant_from_headers,
};
use crate::ingest::RawProfile;

/// One Connect ingest request.
pub(crate) struct ConnectIngest<'a, D> {
    pub(crate) state: &'a DistributorState,
    pub(crate) principal: &'a Principal,
    pub(crate) headers: &'a HeaderMap,
    /// The request payload size. The Connect codec exposes no raw body, so
    /// callers pass the decoded message size as a faithful proxy.
    pub(crate) bytes: u64,
    /// Decodes the request's profiles.
    pub(crate) decode: D,
}

/// Runs one Connect ingest request: resolves and authorizes the tenant,
/// decodes the profiles with `decode`, and appends them to the WAL.
pub(crate) async fn connect_ingest<D>(request: ConnectIngest<'_, D>) -> Result<(), ConnectError>
where
    D: FnOnce() -> Result<Vec<RawProfile>, ProfilesError>,
{
    let ConnectIngest {
        state,
        principal,
        headers,
        bytes,
        decode,
    } = request;
    let start = std::time::Instant::now();
    let tenant = tenant_from_headers(headers, &state.tenant_policy);
    // ONE server span per ingest request (not per sample).
    let ingest_span = ingest_request_span(tenant.as_ref().ok(), bytes);
    let result = async {
        let tenant = tenant.as_ref().map_err(TenantResolveError::clone)?;
        // Before the profiles are decoded, so a denied push reaches no WAL.
        authorize_tenant(principal, tenant)?;
        let raws = decode()?;
        let items = raws.len() as u64;
        process_raw(state, tenant, raws).await?;
        Ok::<u64, ProfilesError>(items)
    }
    .instrument(ingest_span.clone())
    .await;
    record_ingest_outcome(
        state,
        &ingest_span,
        &IngestOutcome {
            tenant: tenant.as_ref().ok(),
            outcome: RequestOutcome::from_result(&result),
            bytes,
            items: *result.as_ref().unwrap_or(&0),
            start,
        },
    );
    result.map(drop).map_err(connect_error)
}
