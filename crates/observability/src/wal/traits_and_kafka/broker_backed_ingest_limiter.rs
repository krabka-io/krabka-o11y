use super::{
    AclEntryFilter, AdminClient, AdminError, BTreeMap, ByteRate, ByteRateExt, ByteSize,
    ByteSizeExt, ClientResourcePolicy, IngestLimitError, IngestQuotaBucket, LogIngestLimiter,
    Mutex, PRODUCER_BYTE_RATE_QUOTA_KEY, Time, WalLogRecord, admin_connection_options, async_trait,
    check_tenant_wal_write_acl, ingest_quota_bytes,
};

pub(crate) struct BrokerBackedIngestLimiter {
    pub(crate) admin: tokio::sync::Mutex<AdminClient>,
    pub(crate) wal_topic: String,
    pub(crate) burst_window: Time,
    pub(crate) buckets: Mutex<BTreeMap<String, IngestQuotaBucket>>,
}

impl BrokerBackedIngestLimiter {
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        burst_window: Time,
    ) -> Result<Self, AdminError> {
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            admin_connection_options(client_resource_policy),
        )
        .await?;
        Ok(Self {
            admin: tokio::sync::Mutex::new(admin),
            wal_topic,
            burst_window,
            buckets: Mutex::new(BTreeMap::new()),
        })
    }
}

#[async_trait]
impl LogIngestLimiter for BrokerBackedIngestLimiter {
    #[cfg_attr(test, mutants::skip)]
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError> {
        let (acls, quota) = {
            let mut admin = self.admin.lock().await;
            let acls = admin
                .describe_acls(&AclEntryFilter::default())
                .await
                .map_err(|error| IngestLimitError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                })?;
            let quota = admin.describe_user_quotas(tenant).await.map_err(|error| {
                IngestLimitError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                }
            })?;
            (acls, quota)
        };
        check_tenant_wal_write_acl(tenant, &self.wal_topic, &acls)?;

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
        let bucket = buckets
            .entry(tenant.to_string())
            .or_insert_with(|| IngestQuotaBucket::new(rate, self.burst_window));
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
}
