use super::{Serialize, Deserialize, SamplePayload, WalExemplar, WalError, Labels};

/// A single metrics WAL record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    pub tenant: String,
    pub labels: Vec<(String, String)>,
    pub payload: SamplePayload,
    pub exemplars: Vec<WalExemplar>,
}

impl WalRecord {
    /// Encodes with `serde-wincode`, which matches the codebase
    /// metadata-record codec.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn encode(&self) -> Result<Vec<u8>, WalError> {
        <serde_wincode::SerdeCompat<WalRecord> as wincode::Serialize>::serialize(self)
            .map_err(|error| WalError::Encode(error.to_string()))
    }

    /// Decodes a [`WalRecord`] from its `serde-wincode` bytes.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, WalError> {
        <serde_wincode::SerdeCompat<WalRecord> as wincode::Deserialize>::deserialize(bytes)
            .map_err(|error| WalError::Decode(error.to_string()))
    }

    /// Series fingerprint from the blockstore's order-independent [`Labels`]
    /// hash.
    #[must_use]
    pub fn series_fingerprint(&self) -> u64 {
        self.labels().fingerprint()
    }

    /// Builds the blockstore label set for this record.
    #[must_use]
    pub fn labels(&self) -> Labels {
        self.labels.iter().cloned().collect()
    }
}
