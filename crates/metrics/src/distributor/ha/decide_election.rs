use super::*;

/// Pure HA election decision against an elected view that is already locked. A
/// caller that holds the tracker lock can decide and commit atomically. A
/// lock-free caller goes through [`ha_election_at_with_timeout`].
pub(crate) fn decide_election(
    elected: &HashMap<(String, String), HaElectionRecord>,
    tenant: &str,
    series: &[DecodedSeries],
    lease_timestamp_ms: i64,
    failover_timeout: Time,
) -> HaElection {
    let Some(first) = series.first() else {
        return HaElection::Accept;
    };
    let Some(replica) = first.labels.get("__replica__") else {
        return HaElection::Accept;
    };
    let cluster = first.labels.get("cluster").unwrap_or("");

    match elected.get(&(tenant.to_string(), cluster.to_string())) {
        Some(elected) if elected.replica == replica => HaElection::Update(HaElectionRecord {
            tenant: tenant.to_string(),
            cluster: cluster.to_string(),
            replica: replica.to_string(),
            lease_timestamp_ms,
        }),
        Some(elected)
            if failover_timeout >= Time::ZERO
                && Time::from_millis(
                    lease_timestamp_ms.saturating_sub(elected.lease_timestamp_ms),
                ) > failover_timeout =>
        {
            HaElection::Elect(HaElectionRecord {
                tenant: tenant.to_string(),
                cluster: cluster.to_string(),
                replica: replica.to_string(),
                lease_timestamp_ms,
            })
        }
        Some(_) => HaElection::Drop,
        None => HaElection::Elect(HaElectionRecord {
            tenant: tenant.to_string(),
            cluster: cluster.to_string(),
            replica: replica.to_string(),
            lease_timestamp_ms,
        }),
    }
}
