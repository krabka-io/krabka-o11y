use super::{
    AdminDenied, AuditEndpoint, AuditOutcome, AuditResource, ConnectInfo, Extensions,
    FromRequestParts, IngestLimitError, MissingPrincipal, OPERATION_TENANT_READ,
    OPERATION_TENANT_WRITE, Parts, PeerAddr, Principal, QueryAuthorizationError, RESOURCE_INGESTER,
    RESOURCE_WAL_TOPIC, ServiceAudit, TenantDenied, TenantId, audit_principal_of, authorize_admin,
    authorize_tenant, resource, source_endpoint, unknown_source_endpoint,
};

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
        let principal = match (extensions.get::<Principal>(), peer) {
            (Some(principal), _) => principal.clone(),
            (None, None) if !audit.authentication_required => Principal::Unauthenticated,
            (None, _) => return Err(MissingPrincipal),
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

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Self::from_extensions(&parts.extensions)
    }
}
