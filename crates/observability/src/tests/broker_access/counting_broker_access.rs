use super::*;

/// A broker that answers from what the test sets, and counts every lookup.
pub(crate) struct CountingBrokerAccess {
    pub(crate) acls: Mutex<Result<AclSet, String>>,
    pub(crate) quotas: Mutex<Result<BTreeMap<String, f64>, String>>,
    pub(crate) acl_lookups: AtomicUsize,
    pub(crate) quota_lookups: AtomicUsize,
}

impl CountingBrokerAccess {
    pub(crate) fn answering(acls: Result<AclSet, String>) -> Self {
        Self {
            acls: Mutex::new(acls),
            quotas: Mutex::new(Ok(BTreeMap::new())),
            acl_lookups: AtomicUsize::new(0),
            quota_lookups: AtomicUsize::new(0),
        }
    }

    pub(crate) fn lookups(&self) -> (usize, usize) {
        (
            self.acl_lookups.load(AtomicOrdering::SeqCst),
            self.quota_lookups.load(AtomicOrdering::SeqCst),
        )
    }
}

#[async_trait]
impl BrokerAccessSource for CountingBrokerAccess {
    async fn wal_topic_acls(&self, _wal_topic: &str) -> Result<AclSet, String> {
        self.acl_lookups.fetch_add(1, AtomicOrdering::SeqCst);
        self.acls.lock().expect("fake ACL lock").clone()
    }

    async fn user_quotas(&self, _tenant: &TenantId) -> Result<BTreeMap<String, f64>, String> {
        self.quota_lookups.fetch_add(1, AtomicOrdering::SeqCst);
        self.quotas.lock().expect("fake quota lock").clone()
    }
}
