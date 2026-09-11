//! The tenant a logs request names, resolved the same way at every boundary.
//!
//! Every push, query, ruler and delete-request handler gets its tenant from
//! this module. The module adds only Grafana Loki's `|` tenant list to
//! [`TenantId::resolve`], so no logs path has a tenant rule of its own. It
//! also holds the responses, because Loki answers the same resolution error
//! with a different status and body on each surface.

use krabka_blockstore::{TENANT_HEADER, TenantId, TenantPolicy, TenantResolveError};

use crate::{
    BTreeSet, Error, HeaderMap, IntoResponse, Response, StatusCode, encode_otlp_status_message,
};

mod grpc_tenant;
mod require_org_id;
mod resolve_federated_tenants;
mod resolve_single_tenant;
mod tenant_error_response;
mod tenant_error_surface;
mod tenant_header_value;
mod tenant_request_error;

pub(crate) use grpc_tenant::grpc_tenant;
pub(crate) use require_org_id::require_org_id;
pub(crate) use resolve_federated_tenants::resolve_federated_tenants;
pub(crate) use resolve_single_tenant::resolve_single_tenant;
pub(crate) use tenant_error_response::tenant_error_response;
pub(crate) use tenant_error_surface::TenantErrorSurface;
pub(crate) use tenant_header_value::tenant_header_value;
pub(crate) use tenant_request_error::TenantRequestError;
