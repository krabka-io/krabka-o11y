use super::*;

pub(crate) struct BrokerBackedQueryAuthorizer {
    pub(crate) admin: tokio::sync::Mutex<AdminClient>,
    pub(crate) wal_topic: String,
    pub(crate) connected: Arc<AtomicBool>,
}

impl BrokerBackedQueryAuthorizer {
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        connected: Arc<AtomicBool>,
    ) -> Result<Self, AdminError> {
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            admin_connection_options(client_resource_policy),
        )
        .await?;
        Ok(Self {
            admin: tokio::sync::Mutex::new(admin),
            wal_topic,
            connected,
        })
    }
}

#[async_trait]
impl LogQueryAuthorizer for BrokerBackedQueryAuthorizer {
    #[cfg_attr(test, mutants::skip)]
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        let result = {
            let mut admin = self.admin.lock().await;
            admin.describe_acls(&AclEntryFilter::default()).await
        };
        let acls = match result {
            Ok(acls) => {
                self.connected.store(true, AtomicOrdering::SeqCst);
                acls
            }
            Err(error) => {
                self.connected.store(false, AtomicOrdering::SeqCst);
                return Err(QueryAuthorizationError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                });
            }
        };
        check_tenant_wal_read_acl(tenant, &self.wal_topic, &acls)
    }
}
