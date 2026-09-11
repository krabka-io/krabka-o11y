//! The principal, the client address and the audit trail of one request.
//!
//! The authentication layer of [`crate::server_security`] puts a [`Principal`]
//! into every request that it passes on. `serve_router` gives the request the
//! [`PeerAddr`] of its connection. The service runtime adds the
//! [`ServiceAudit`] of the process. A handler reads the three together as one
//! [`RequestSecurity`]. It authorizes each tenant through that value, and it
//! records each audited operation through the same value.

use axum::{
    Extension, Router,
    extract::{ConnectInfo, FromRequestParts},
    http::{Extensions, StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use krabka_blockstore::TenantId;

use crate::{
    Arc, Error, IngestLimitError, QueryAuthorizationError,
    audit::{
        AuditEndpoint, AuditHandle, AuditOutcome, AuditResource, OPERATION_TENANT_READ,
        OPERATION_TENANT_WRITE, RESOURCE_INGESTER, RESOURCE_WAL_TOPIC, audit_principal_of,
        resource, source_endpoint, unknown_source_endpoint,
    },
    server_security::{
        AdminDenied, PeerAddr, Principal, TenantDenied, authorize_admin, authorize_tenant,
    },
};

mod missing_principal;
mod request_security;
mod service_audit;
mod with_service_audit;

pub(crate) use self::{
    missing_principal::MissingPrincipal, request_security::RequestSecurity,
    service_audit::ServiceAudit, with_service_audit::with_service_audit,
};
