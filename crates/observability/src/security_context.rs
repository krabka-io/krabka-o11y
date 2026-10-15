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

/// `router`, with `audit` in the extensions of every request that it serves.
///
/// Call it on the finished router. A route that is merged in later does not
/// get the extension.
pub(crate) fn with_service_audit(router: Router, audit: ServiceAudit) -> Router {
    router.layer(Extension(audit))
}

/// A request reached a handler that needs a principal, but it carries none.
///
/// The authentication layer gives every request a principal, except on a
/// route that skips authentication. So a handler that needs a principal on
/// such a route answers 500, and a route put on that list by mistake fails
/// closed. A service that reads a credentials file also answers 500 for a
/// request that came through no listener, so a router served without the
/// authentication layer fails closed too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the request has no principal from the authentication layer")]
pub(crate) struct MissingPrincipal;

impl IntoResponse for MissingPrincipal {
    fn into_response(self) -> Response {
        crate::text_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{self}\n"))
    }
}

/// Who sent one request, from which address, and the audit trail that records
/// the decisions about it.
#[derive(Clone, Debug)]
pub(crate) struct RequestSecurity {
    /// The principal that the authentication layer decided.
    pub(crate) principal: Principal,
    /// The client address, or `0.0.0.0:0` for a request with no connection.
    pub(crate) source: AuditEndpoint,
    /// The audit trail of the service.
    pub(crate) audit: ServiceAudit,
}

impl RequestSecurity {
    /// The security of a request that reached a router in process, with no
    /// listener, no authentication layer and no audit trail.
    #[cfg(test)]
    pub(crate) fn unauthenticated() -> Self {
        Self {
            principal: Principal::Unauthenticated,
            source: unknown_source_endpoint(),
            audit: ServiceAudit::disabled(),
        }
    }

    /// Reads the security of a request from its extensions.
    ///
    /// A request that came through a `ServerListener` carries
    /// `ConnectInfo<PeerAddr>`, and it passed the authentication layer. That
    /// layer gives a principal to every request on a route that needs
    /// authentication. A request from a listener with no principal is on a
    /// route that skips authentication, so this refuses it.
    ///
    /// A request with no `ConnectInfo<PeerAddr>` did not come through a
    /// listener. When the service reads no credentials file, it is
    /// unauthenticated, which is the upstream posture and what an in-process
    /// router in a test needs. When the service does read a credentials file,
    /// this refuses it: the service serves that router some other way than
    /// through `serve_router`, and treating the request as unauthenticated
    /// would skip the credentials check.
    ///
    /// # Errors
    ///
    /// Returns [`MissingPrincipal`] for a request without a principal that came
    /// through a listener, or that came through none while authentication is
    /// required.
    pub(crate) fn from_extensions(extensions: &Extensions) -> Result<Self, MissingPrincipal> {
        let peer = extensions
            .get::<ConnectInfo<PeerAddr>>()
            .map(|ConnectInfo(peer)| peer);
        let audit = extensions
            .get::<ServiceAudit>()
            .cloned()
            .unwrap_or_else(ServiceAudit::disabled);
        let principal = if let Some(principal) = extensions.get::<Principal>() {
            principal.clone()
        } else if peer.is_none() && !audit.authentication_required {
            Principal::Unauthenticated
        } else {
            return Err(MissingPrincipal);
        };
        Ok(Self {
            principal,
            source: peer.map_or_else(unknown_source_endpoint, |peer| source_endpoint(peer.socket)),
            audit,
        })
    }

    /// Checks that the principal may use `tenant`.
    ///
    /// # Errors
    ///
    /// Returns [`TenantDenied`] when the grant of an authenticated principal
    /// does not include `tenant`. The security events record the denial.
    pub(crate) fn authorize_tenant(&self, tenant: &TenantId) -> Result<(), TenantDenied> {
        authorize_tenant(&self.principal, tenant)
    }

    /// Checks that the principal may call an admin operation.
    ///
    /// # Errors
    ///
    /// Returns [`AdminDenied`] for an authenticated principal without `admin`.
    /// The security events record the denial.
    pub(crate) fn authorize_admin(&self) -> Result<(), AdminDenied> {
        authorize_admin(&self.principal)
    }

    /// Records an operation that the service tried on a tenant's data or on
    /// the service itself.
    pub(crate) fn admin_operation(
        &self,
        operation: &'static str,
        resources: Vec<AuditResource>,
        outcome: AuditOutcome,
    ) {
        self.audit.handle.admin_operation(
            audit_principal_of(&self.principal),
            self.source.clone(),
            operation,
            resources,
            outcome,
        );
    }

    /// The resource that names the ingester of this process.
    pub(crate) fn ingester(&self) -> AuditResource {
        resource(RESOURCE_INGESTER, self.audit.instance.as_ref())
    }

    /// Records a read that the broker ACLs refused.
    ///
    /// Only [`QueryAuthorizationError::Unauthorized`] is a refusal. A check
    /// that could not reach the broker records nothing.
    pub(crate) fn record_read_refusal(&self, error: &QueryAuthorizationError) {
        if matches!(error, QueryAuthorizationError::Unauthorized { .. }) {
            self.wal_topic_denied(OPERATION_TENANT_READ);
        }
    }

    /// Records a write that the broker ACLs refused.
    ///
    /// Only [`IngestLimitError::Unauthorized`] is a refusal. A rate limit and
    /// a check that could not reach the broker record nothing.
    pub(crate) fn record_write_refusal(&self, error: &IngestLimitError) {
        if matches!(error, IngestLimitError::Unauthorized { .. }) {
            self.wal_topic_denied(OPERATION_TENANT_WRITE);
        }
    }

    fn wal_topic_denied(&self, operation: &'static str) {
        self.audit.handle.authorization_denied(
            audit_principal_of(&self.principal),
            self.source.clone(),
            RESOURCE_WAL_TOPIC,
            self.audit.wal_topic.as_ref(),
            operation,
        );
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RequestSecurity {
    type Rejection = MissingPrincipal;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(Self::from_extensions(&parts.extensions))
    }
}

/// The audit handle of a service, with the names that its audit events use.
///
/// The service runtime puts one into the extensions of every request. A
/// router that runs in process, without the runtime, has none, and its
/// requests use [`ServiceAudit::disabled`].
#[derive(Clone, Debug)]
pub(crate) struct ServiceAudit {
    /// The handle that records the events.
    pub(crate) handle: AuditHandle,
    /// The WAL topic that a broker ACL refusal names.
    pub(crate) wal_topic: Arc<str>,
    /// The instance that an ingester operation names.
    ///
    /// This is the instance that the role's `/ring` page shows.
    pub(crate) instance: Arc<str>,
    /// Whether the service reads a credentials file.
    ///
    /// When it does, a request that carries no principal and came through no
    /// listener is refused. Such a request reached a router that the service
    /// serves some other way than through `serve_router`, which would skip the
    /// credentials check.
    pub(crate) authentication_required: bool,
}

impl ServiceAudit {
    /// An audit trail that records nothing.
    pub(crate) fn disabled() -> Self {
        Self {
            handle: AuditHandle::disabled(),
            wal_topic: Arc::from(""),
            instance: Arc::from(""),
            authentication_required: false,
        }
    }
}
