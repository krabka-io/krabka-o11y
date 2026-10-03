use super::{
    BTreeSet, DEFAULT_BLOCK_SWEEP_GRACE, StorageAuditError, StorageAuditOptions,
    StorageFindingKind, StorageSignal, SystemTime, Time,
};

/// The explicit scope of one repair run.
///
/// A repair acts on one tenant of one signal, and only on the finding kinds
/// it names. Nothing here has a default that widens the scope.
#[derive(Clone, Debug)]
pub struct RepairOptions {
    pub tenant: String,
    pub signal: StorageSignal,
    /// The finding kinds to act on. Each one must be repairable.
    pub kinds: BTreeSet<StorageFindingKind>,
    /// Without `apply`, the repair only plans.
    pub apply: bool,
    /// The grace window of the audit the repair runs first, and of the
    /// recheck before each delete.
    pub grace: Time,
    pub now: SystemTime,
    pub trace_index_key: String,
    pub profile_index_key: String,
}

impl RepairOptions {
    /// A plan-only repair of `kinds` for `tenant` of `signal`, with the
    /// default grace window and index keys.
    #[must_use]
    pub fn new(
        tenant: impl Into<String>,
        signal: StorageSignal,
        kinds: BTreeSet<StorageFindingKind>,
        now: SystemTime,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            signal,
            kinds,
            apply: false,
            grace: DEFAULT_BLOCK_SWEEP_GRACE,
            now,
            trace_index_key: "index/traces.json".to_string(),
            profile_index_key: "index/profiles.json".to_string(),
        }
    }

    /// Refuses a scope that is empty or that names a kind no repair acts on.
    ///
    /// # Errors
    /// Returns [`StorageAuditError::InvalidScope`].
    pub fn validate(&self) -> Result<(), StorageAuditError> {
        if self.tenant.is_empty() {
            return Err(StorageAuditError::InvalidScope(
                "a repair needs a tenant".to_string(),
            ));
        }
        if self.kinds.is_empty() {
            return Err(StorageAuditError::InvalidScope(
                "a repair needs at least one finding kind".to_string(),
            ));
        }
        if let Some(kind) = self.kinds.iter().find(|kind| !kind.is_repairable()) {
            return Err(StorageAuditError::InvalidScope(format!(
                "finding kind `{kind}` needs a person, not a repair; \
                 repairable kinds are orphan and orphan_sidecar"
            )));
        }
        Ok(())
    }

    /// The options of the audit that the repair runs first.
    #[must_use]
    pub fn audit_options(&self) -> StorageAuditOptions {
        StorageAuditOptions {
            signal: Some(self.signal),
            tenant: Some(self.tenant.clone()),
            grace: self.grace,
            verify_data: false,
            now: self.now,
            trace_index_key: self.trace_index_key.clone(),
            profile_index_key: self.profile_index_key.clone(),
        }
    }
}
