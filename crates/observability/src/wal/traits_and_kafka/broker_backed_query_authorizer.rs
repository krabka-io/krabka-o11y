use super::{
    AdminBrokerAccess, AdminClient, AdminError, Arc, AtomicBool, BrokerAccessCache,
    BrokerAccessPolicy, CancellationToken, ClientResourcePolicy, ClientSecurity,
    LogQueryAuthorizer, Principal, QueryAuthorizationError, TenantId, admin_connection_options,
    async_trait, check_tenant_wal_read_acl,
};

/// Allows a read when the broker's ACLs grant the request's ACL principal read
/// on the WAL topic.
///
/// The ACL principal is `User:{name}` for an authenticated request and
/// `User:{tenant}` otherwise. [`check_tenant_wal_read_acl`] gives the whole
/// rule. The ACLs come from a [`BrokerAccessCache`], so a check does not wait
/// on the broker. See that type for how stale an answer may be.
pub(crate) struct BrokerBackedQueryAuthorizer {
    pub(crate) access: BrokerAccessCache,
}

impl BrokerBackedQueryAuthorizer {
    /// Connects the admin client under `security`, and holds its answers
    /// under `policy`.
    ///
    /// `connected` reads `true` while the last ACL lookup succeeded.
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        security: Option<&ClientSecurity>,
        connected: Arc<AtomicBool>,
        policy: BrokerAccessPolicy,
    ) -> Result<Self, AdminError> {
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            admin_connection_options(client_resource_policy, security),
        )
        .await?;
        let source = Arc::new(AdminBrokerAccess {
            admin: tokio::sync::Mutex::new(admin),
        });
        Ok(Self {
            access: BrokerAccessCache::new(source, wal_topic, policy, connected),
        })
    }

    /// Refreshes the ACL snapshot once per TTL until `token` is cancelled.
    pub(crate) async fn keep_fresh(&self, token: CancellationToken) {
        self.access.keep_fresh(token).await;
    }
}

#[async_trait]
impl LogQueryAuthorizer for BrokerBackedQueryAuthorizer {
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        let acls =
            self.access
                .acls()
                .await
                .map_err(|reason| QueryAuthorizationError::Unavailable {
                    tenant: tenant.to_string(),
                    reason,
                })?;
        check_tenant_wal_read_acl(principal, tenant, &self.access.wal_topic, &acls)
    }
}
