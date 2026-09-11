use krabka_client_core::ClientError;

use super::{
    AclSet, AdminClient, AdminError, BTreeMap, BrokerAccessSource, TenantId, acl_set_from_describe,
    async_trait, wal_topic_acl_filters,
};

/// A [`BrokerAccessSource`] that asks a broker over one admin connection.
///
/// The connection is behind an async mutex, so the lookups are serial. Only
/// [`BrokerAccessCache`](super::BrokerAccessCache) calls this, at most once
/// per TTL for the ACLs and once per TTL for each tenant's quota, so no
/// request waits in a queue on this mutex.
pub(crate) struct AdminBrokerAccess {
    pub(crate) admin: tokio::sync::Mutex<AdminClient>,
}

#[async_trait]
impl BrokerAccessSource for AdminBrokerAccess {
    #[cfg_attr(test, mutants::skip)]
    async fn wal_topic_acls(&self, wal_topic: &str) -> Result<AclSet, String> {
        let mut admin = self.admin.lock().await;
        let mut entries = Vec::new();
        for filter in wal_topic_acl_filters(wal_topic) {
            match acl_set_from_describe(admin.describe_acls(&filter).await) {
                Ok(AclSet::Configured(found)) => entries.extend(found),
                Ok(AclSet::SecurityDisabled) => return Ok(AclSet::SecurityDisabled),
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(AclSet::Configured(entries))
    }

    #[cfg_attr(test, mutants::skip)]
    async fn user_quotas(&self, tenant: &TenantId) -> Result<BTreeMap<String, f64>, String> {
        let mut admin = self.admin.lock().await;
        match admin.describe_user_quotas(tenant.as_str()).await {
            Ok(quota) => Ok(quota),
            // A broker without client quotas: error 35 is
            // `UNSUPPORTED_VERSION`, and a client that cannot speak API 48 at
            // all never sends the request. Neither has a quota to enforce.
            Err(
                AdminError::Broker {
                    api: "DescribeClientQuotas",
                    code: 35,
                    ..
                }
                | AdminError::Transport(ClientError::IncompatibleVersion { api_key: 48, .. }),
            ) => Ok(BTreeMap::new()),
            Err(error) => Err(error.to_string()),
        }
    }
}
