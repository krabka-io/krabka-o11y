use super::{Bytes, HaElectionRecord};

#[must_use]
pub fn ha_election_compaction_key(record: &HaElectionRecord) -> Bytes {
    Bytes::from(format!("{}\0{}", record.tenant, record.cluster))
}
