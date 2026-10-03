use super::{
    DEFAULT_BLOCK_SWEEP_GRACE, StorageAuditScope, StorageSignal, SystemTime, Time, TimeExt,
};

/// What a storage audit looks at and how it judges age.
#[derive(Clone, Debug)]
pub struct StorageAuditOptions {
    /// Audit one signal only. `None` audits all four.
    pub signal: Option<StorageSignal>,
    /// Report on one tenant only. `None` reports on every tenant. Shared
    /// state, such as a fleet-wide index, is in every report.
    pub tenant: Option<String>,
    /// An object that no index names is `pending` while it is younger than
    /// this, and `orphan` after.
    pub grace: Time,
    /// Read every block in full, not only its Parquet footer.
    pub verify_data: bool,
    /// The instant that ages are measured from.
    pub now: SystemTime,
    /// The trace index key, as the traces service is configured.
    pub trace_index_key: String,
    /// The profile index key, as the profiles service is configured.
    pub profile_index_key: String,
}

impl StorageAuditOptions {
    /// Options that audit every signal and every tenant at `now`, with the
    /// default grace window and index keys.
    #[must_use]
    pub fn new(now: SystemTime) -> Self {
        Self {
            signal: None,
            tenant: None,
            grace: DEFAULT_BLOCK_SWEEP_GRACE,
            verify_data: false,
            now,
            trace_index_key: "index/traces.json".to_string(),
            profile_index_key: "index/profiles.json".to_string(),
        }
    }

    /// The scope that a report of these options records.
    #[must_use]
    pub fn scope(&self) -> StorageAuditScope {
        StorageAuditScope {
            signal: self.signal,
            tenant: self.tenant.clone(),
            grace_secs: u64::try_from(self.grace.secs_i64()).unwrap_or(0),
            verify_data: self.verify_data,
        }
    }

    /// Whether the audit covers `signal`.
    #[must_use]
    pub fn covers_signal(&self, signal: StorageSignal) -> bool {
        self.signal.is_none_or(|wanted| wanted == signal)
    }

    /// Whether the audit reports on an object owned by `tenant`. Shared
    /// state, with no tenant, is always in scope.
    #[must_use]
    pub fn covers_tenant(&self, tenant: Option<&str>) -> bool {
        match (&self.tenant, tenant) {
            (Some(wanted), Some(tenant)) => wanted == tenant,
            _ => true,
        }
    }
}
