use super::*;

/// A single profiles WAL record: one tenant, one series, one profile type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileRecord {
    pub tenant: String,
    pub labels: Vec<(String, String)>,
    pub profile_type: String,
    pub samples: Vec<WalSample>,
    pub symbols: WalSymbolSet,
}

impl ProfileRecord {
    /// Encode with `serde-wincode`.
    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn encode(&self) -> Result<Vec<u8>, ProfilesError> {
        <SerdeCompat<Self> as WincodeSerialize>::serialize(self)
            .map_err(|err| ProfilesError::Wal(err.to_string()))
    }

    /// Decode from `serde-wincode` bytes.
    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProfilesError> {
        <SerdeCompat<Self> as WincodeDeserialize>::deserialize(bytes)
            .map_err(|err| ProfilesError::Wal(err.to_string()))
    }

    /// Series fingerprint from blockstore `Labels`, independent of label order.
    #[must_use]
    pub fn series_fingerprint(&self) -> u64 {
        let mut labels = Labels::new();
        for (name, value) in &self.labels {
            labels.insert(name.clone(), value.clone());
        }
        labels.fingerprint()
    }
}
