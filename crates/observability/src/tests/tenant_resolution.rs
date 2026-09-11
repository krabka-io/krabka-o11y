use axum::body::to_bytes;

use super::prelude::{
    AllowAllIngestLimiter, BTreeMap, IntoResponse, Response, StatusCode, TenantErrorSurface,
    TenantId, TenantIdError, TenantRequestError, TenantResolveError, WalLogRecord, check,
    check_ingest_quota, grpc_tenant, resolve_federated_tenants, resolve_single_tenant,
    tenant_error_response,
};
use crate::{
    DistributorError, IngestLimitError, LogIngestLimiter, async_trait, server_security::Principal,
};

mod a_batch_with_a_record_for_another_tenant_never_reaches_the_limiter;
mod a_federated_header_resolves_every_part_sorted_and_without_repeats;
mod a_grpc_export_without_one_valid_tenant_gets_a_status_code;
mod a_single_tenant_header_resolves_as_dskit_resolves_it;
mod every_tenant_error_surface_answers_as_loki_does;
