use std::collections::BTreeMap;

use super::{Deserialize, Labels, MetricString, SamplePayload, Serialize, WalError, WalExemplar};

/// A single metrics WAL record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    pub tenant: String,
    pub labels: Vec<(String, MetricString)>,
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
        let pairs = self
            .labels
            .iter()
            .map(|(name, value)| (name.as_str(), value));
        if self.labels.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            fingerprint_label_pairs(pairs)
        } else {
            // The label map sorts names and keeps the last duplicate value.
            fingerprint_label_pairs(pairs.collect::<BTreeMap<_, _>>().into_iter())
        }
    }

    /// Builds the blockstore label set for this record.
    #[must_use]
    pub fn labels(&self) -> Labels {
        self.labels.iter().cloned().collect()
    }
}

fn fingerprint_label_pairs<'a>(pairs: impl Iterator<Item = (&'a str, &'a MetricString)>) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for (name, value) in pairs {
        for bytes in [name.as_bytes(), value.as_bytes()] {
            for byte in (bytes.len() as u64).to_le_bytes() {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
            for &byte in bytes {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
    }
    hash
}
