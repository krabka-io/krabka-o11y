//! Profiles WAL topic record contract.

use bytes::Bytes;
use krabka_blockstore::{LabelPair, Labels};
use krabka_client_consumer::ConsumerRecord;
/// The profiles WAL topic name. The name, partition count, and retention
/// window are maintained together in the shared topic contract.
pub use krabka_observability::topic_contract::PROFILES_WAL_TOPIC;
use serde::{Deserialize, Serialize};
use serde_wincode::SerdeCompat;
use sha2::{Digest, Sha256};
use wincode::{Deserialize as WincodeDeserialize, Serialize as WincodeSerialize};

use crate::error::ProfilesError;

/// A wire-compatible boolean flag that [`WalMapping`] uses.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WalFlag(pub(crate) bool);

impl WalFlag {
    #[must_use]
    pub const fn get(self) -> bool {
        self.0
    }
}

impl From<bool> for WalFlag {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

/// A function entry; string fields are indices into `WalSymbolSet.strings`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFunction {
    pub name: u32,
    pub system_name: u32,
    pub filename: u32,
    pub start_line: i64,
}

/// A line in a location, with its function reference and source line number.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalLine {
    pub function_id: u32,
    pub line: i64,
}

/// A location: an address plus lines `(function_id, line)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalLocation {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<WalLine>,
}

/// A mapping. A false `has_functions` flag marks an unsymbolized mapping.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalMapping {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub has_functions: WalFlag,
    pub has_filenames: WalFlag,
    pub has_line_numbers: WalFlag,
    pub has_inline_frames: WalFlag,
}

/// One sample's raw payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSample {
    pub stacktrace_location_refs: Vec<u32>,
    pub value: i64,
    pub timestamp_ns: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

/// The profile's symbol tables, index-encoded in pprof shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSymbolSet {
    pub strings: Vec<String>,
    pub functions: Vec<WalFunction>,
    pub locations: Vec<WalLocation>,
    pub mappings: Vec<WalMapping>,
}

impl WalSymbolSet {
    /// Tables for a profile with one frame, named by `strings[1]`.
    #[must_use]
    pub fn single_frame(function: String) -> Self {
        Self {
            strings: vec![String::new(), function],
            functions: vec![WalFunction {
                name: 1,
                system_name: 1,
                filename: 0,
                start_line: 0,
            }],
            locations: vec![WalLocation {
                address: 0,
                mapping_id: 0,
                lines: vec![WalLine {
                    function_id: 0,
                    line: 1,
                }],
            }],
            mappings: Vec::new(),
        }
    }
}

/// A single profiles WAL record: one tenant, one series, one profile type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileRecord {
    pub tenant: String,
    pub labels: Vec<LabelPair>,
    pub profile_type: String,
    pub samples: Vec<WalSample>,
    pub symbols: WalSymbolSet,
}

impl ProfileRecord {
    /// Encode with `serde-wincode`.
    ///
    /// # Errors
    /// Returns an error when serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>, ProfilesError> {
        <SerdeCompat<Self> as WincodeSerialize>::serialize(self)
            .map_err(|err| ProfilesError::Wal(err.to_string()))
    }

    /// Decode from `serde-wincode` bytes.
    ///
    /// # Errors
    /// Returns an error when deserialization fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProfilesError> {
        <SerdeCompat<Self> as WincodeDeserialize>::deserialize(bytes)
            .map_err(|err| ProfilesError::Wal(err.to_string()))
    }

    /// Series fingerprint from blockstore `Labels`, independent of label order.
    #[must_use]
    pub fn series_fingerprint(&self) -> u64 {
        Labels::from_pairs(self.labels.iter().cloned()).fingerprint()
    }
}

/// The topic and payload distinguish reused offsets from unrelated WAL data.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WalPosition {
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) record_hash: [u8; 32],
}

impl WalPosition {
    pub(crate) fn from_record(record: &ConsumerRecord, value: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update(record.topic.as_bytes());
        hash.update([0]);
        hash.update(value);
        Self {
            partition: record.partition,
            offset: record.offset,
            record_hash: hash.finalize().into(),
        }
    }

    pub(crate) fn sample_identity(self, ordinal: u64) -> Vec<u8> {
        [
            self.record_hash.as_slice(),
            self.partition.to_be_bytes().as_slice(),
            self.offset.to_be_bytes().as_slice(),
            ordinal.to_be_bytes().as_slice(),
        ]
        .concat()
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn symbols() -> WalSymbolSet {
        WalSymbolSet {
            strings: vec![String::new(), "main".to_string(), "main.go".to_string()],
            functions: vec![WalFunction {
                name: 1,
                system_name: 1,
                filename: 2,
                start_line: 10,
            }],
            locations: vec![WalLocation {
                address: 0x40,
                mapping_id: 0,
                lines: vec![WalLine {
                    function_id: 0,
                    line: 12,
                }],
            }],
            mappings: vec![WalMapping {
                memory_start: 0,
                memory_limit: 0x1000,
                file_offset: 0,
                filename: 2,
                build_id: 0,
                has_functions: true.into(),
                has_filenames: true.into(),
                has_line_numbers: true.into(),
                has_inline_frames: false.into(),
            }],
        }
    }

    fn record() -> ProfileRecord {
        ProfileRecord {
            tenant: "t1".to_string(),
            labels: vec![
                LabelPair::new("__name__", "process_cpu"),
                LabelPair::new("service_name", "api"),
            ],
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0],
                value: 1500,
                timestamp_ns: 1_700_000_000_000_000_000,
                span_id: Some(42),
                trace_id: Some(vec![0xaa; 16]),
            }],
            symbols: symbols(),
        }
    }

    #[test]
    fn record_round_trips() {
        let record = record();
        let bytes = record.encode().unwrap();
        let decoded = ProfileRecord::decode(&bytes).unwrap();
        assert!(decoded == record);
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = record();
        let mut b = a.clone();
        b.labels = vec![
            LabelPair::new("service_name", "api"),
            LabelPair::new("__name__", "process_cpu"),
        ];
        assert!(a.series_fingerprint() == b.series_fingerprint());
    }

    #[test]
    fn partition_key_is_stable_and_distinct() {
        let k1 = partition_key("t", 42);
        let k2 = partition_key("t", 42);
        let k3 = partition_key("t", 43);
        let k4 = partition_key("u", 42);
        check!(k1 == k2);
        check!(k1 != k3);
        check!(k1 != k4);
    }
}

/// Produce key for a WAL record: deterministic `(tenant, fingerprint)` bytes.
#[must_use]
pub fn partition_key(tenant: &str, fingerprint: u64) -> Bytes {
    let mut buf = Vec::with_capacity(tenant.len() + 8);
    buf.extend_from_slice(tenant.as_bytes());
    buf.extend_from_slice(&fingerprint.to_be_bytes());
    Bytes::from(buf)
}
