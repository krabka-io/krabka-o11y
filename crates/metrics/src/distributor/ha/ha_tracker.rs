use super::{Mutex, HashMap, HaElectionRecord, now_ms, DecodedSeries, HaElection, DEFAULT_HA_FAILOVER_TIMEOUT, Time, decide_election};

/// In-memory elected replica view. The distributor rebuilds it from the
/// compacted HA-tracker topic, and extends it with an in-process first-seen
/// election for unseen pairs.
#[derive(Debug, Default)]
pub struct HaTracker {
    pub(crate) elected: Mutex<HashMap<(String, String), HaElectionRecord>>,
}

impl HaTracker {
    #[must_use]
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn elected_replica(&self, tenant: &str, cluster: &str) -> Option<String> {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .get(&(tenant.to_string(), cluster.to_string()))
            .map(|record| record.replica.clone())
    }

    #[must_use]
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn election_record(&self, tenant: &str, cluster: &str) -> Option<HaElectionRecord> {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .get(&(tenant.to_string(), cluster.to_string()))
            .cloned()
    }

    pub fn set_elected(
        &self,
        tenant: impl Into<String>,
        cluster: impl Into<String>,
        replica: impl Into<String>,
    ) {
        self.set_elected_at(tenant, cluster, replica, now_ms());
    }

    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn set_elected_at(
        &self,
        tenant: impl Into<String>,
        cluster: impl Into<String>,
        replica: impl Into<String>,
        lease_timestamp_ms: i64,
    ) {
        let tenant = tenant.into();
        let cluster = cluster.into();
        let replica = replica.into();
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .insert(
                (tenant.clone(), cluster.clone()),
                HaElectionRecord {
                    tenant,
                    cluster,
                    replica,
                    lease_timestamp_ms,
                },
            );
    }

    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn persist_elected(&self, record: &HaElectionRecord) {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .insert(
                (record.tenant.clone(), record.cluster.clone()),
                record.clone(),
            );
    }

    /// Decides and commits the HA election for `series` atomically, with the
    /// current wall clock and the default failover timeout. See
    /// [`Self::elect`].
    pub fn elect_now(&self, tenant: &str, series: &[DecodedSeries]) -> HaElection {
        self.elect(tenant, series, now_ms(), DEFAULT_HA_FAILOVER_TIMEOUT)
    }

    /// Decides and commits the HA election atomically, with the current wall
    /// clock and the supplied failover timeout.
    pub fn elect_now_with_timeout(
        &self,
        tenant: &str,
        series: &[DecodedSeries],
        failover_timeout: Time,
    ) -> HaElection {
        self.elect(tenant, series, now_ms(), failover_timeout)
    }

    /// Decides the HA election for `series` atomically. When the decision is
    /// `Elect` or `Update`, it commits the in-memory winner under the same
    /// lock. This closes the elect TOCTOU, because a second racing replica that
    /// locks afterwards sees the committed winner and is dropped. The DURABLE
    /// Kafka persist stays with the caller and can proceed asynchronously after
    /// this function returns.
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn elect(
        &self,
        tenant: &str,
        series: &[DecodedSeries],
        lease_timestamp_ms: i64,
        failover_timeout: Time,
    ) -> HaElection {
        let mut elected = self.elected.lock().expect("HaTracker mutex poisoned");
        let decision = decide_election(
            &elected,
            tenant,
            series,
            lease_timestamp_ms,
            failover_timeout,
        );
        if let HaElection::Elect(record) | HaElection::Update(record) = &decision {
            elected.insert(
                (record.tenant.clone(), record.cluster.clone()),
                record.clone(),
            );
        }
        decision
    }
}
