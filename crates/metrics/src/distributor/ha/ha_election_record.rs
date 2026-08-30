use super::HaElectionRecordError;

/// A persisted HA election record for the compacted HA-tracker topic.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HaElectionRecord {
    pub tenant: String,
    pub cluster: String,
    pub replica: String,
    pub lease_timestamp_ms: i64,
}

impl HaElectionRecord {
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn encode(&self) -> Result<Vec<u8>, HaElectionRecordError> {
        serde_json::to_vec(self).map_err(|error| HaElectionRecordError::Encode(error.to_string()))
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, HaElectionRecordError> {
        serde_json::from_slice(bytes)
            .map_err(|error| HaElectionRecordError::Decode(error.to_string()))
    }
}
