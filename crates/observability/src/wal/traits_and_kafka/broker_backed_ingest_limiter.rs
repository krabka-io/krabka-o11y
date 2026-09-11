use super::{
    AdminBrokerAccess, AdminClient, AdminError, Arc, AtomicBool, BrokerAccessCache,
    BrokerAccessPolicy, ByteRate, ByteRateExt, ByteSize, ByteSizeExt, CancellationToken,
    ClientResourcePolicy, ClientSecurity, IngestLimitError, IngestQuotaBucket, LogIngestLimiter,
    Mutex, PRODUCER_BYTE_RATE_QUOTA_KEY, Principal, TenantId, TenantLru, Time, WalLogRecord,
    admin_connection_options, async_trait, check_tenant_wal_write_acl, ingest_quota_bytes,
};

/// Allows a push when the broker's ACLs grant the request's ACL principal
/// write on the WAL topic, and the push fits the tenant's `producer_byte_rate`
/// quota.
///
/// The ACL principal is `User:{name}` for an authenticated request and
/// `User:{tenant}` otherwise. [`check_tenant_wal_write_acl`] gives the whole
/// rule. The ACLs and the quota come from a [`BrokerAccessCache`]. The rate buckets
/// hold at most the policy's tenant capacity. A tenant whose bucket is removed
/// to make room gets a full bucket on its next push, which allows one burst
/// window of bytes early.
pub(crate) struct BrokerBackedIngestLimiter {
    pub(crate) access: BrokerAccessCache,
    pub(crate) burst_window: Time,
    pub(crate) buckets: Mutex<TenantLru<IngestQuotaBucket>>,
}

impl BrokerBackedIngestLimiter {
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        security: Option<&ClientSecurity>,
        burst_window: Time,
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
        Ok(Self::with_access(
            BrokerAccessCache::new(source, wal_topic, policy, Arc::new(AtomicBool::new(false))),
            burst_window,
        ))
    }

    pub(crate) fn with_access(access: BrokerAccessCache, burst_window: Time) -> Self {
        let capacity = access.policy.tenant_capacity;
        Self {
            access,
            burst_window,
            buckets: Mutex::new(TenantLru::new(capacity)),
        }
    }
}

#[async_trait]
impl LogIngestLimiter for BrokerBackedIngestLimiter {
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
        records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        let unavailable = |reason| IngestLimitError::Unavailable {
            tenant: tenant.to_string(),
            reason,
        };
        let acls = self.access.acls().await.map_err(unavailable)?;
        check_tenant_wal_write_acl(principal, tenant, &self.access.wal_topic, &acls)?;
        let quota = self.access.quotas(tenant).await.map_err(unavailable)?;

        let Some(raw_rate) = quota.get(PRODUCER_BYTE_RATE_QUOTA_KEY).copied() else {
            return Ok(());
        };
        if !raw_rate.is_finite() || raw_rate <= 0.0 {
            return Ok(());
        }
        let rate = ByteRate::from_bytes_per_sec_f64(raw_rate);

        let batch = ingest_quota_bytes(records);
        if batch == <ByteSize as ByteSizeExt>::ZERO {
            return Ok(());
        }

        let mut buckets = self.buckets.lock().expect("ingest quota lock poisoned");
        let bucket =
            buckets.get_or_insert_with(tenant, || IngestQuotaBucket::new(rate, self.burst_window));
        bucket.update_rate(rate);
        if bucket.consume(batch) {
            return Ok(());
        }

        let (rate, bytes) = (rate.bytes_per_sec_f64(), batch.bytes_usize());
        Err(IngestLimitError::RateLimited {
            tenant: tenant.to_string(),
            reason: format!(
                "{PRODUCER_BYTE_RATE_QUOTA_KEY} quota {rate:.0} bytes/s exceeded by {bytes} byte ingest batch"
            ),
        })
    }

    async fn keep_fresh(&self, token: CancellationToken) {
        self.access.keep_fresh(token).await;
    }
}
