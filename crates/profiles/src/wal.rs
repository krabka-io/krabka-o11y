//! Profiles WAL topic record contract.

use bytes::Bytes;
use krabka_blockstore::Labels;
use serde::{Deserialize, Serialize};
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as WincodeDeserialize, Serialize as WincodeSerialize};

use crate::error::ProfilesError;

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
                lines: vec![(0, 12)],
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
                ("__name__".to_string(), "process_cpu".to_string()),
                ("service_name".to_string(), "api".to_string()),
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
            ("service_name".to_string(), "api".to_string()),
            ("__name__".to_string(), "process_cpu".to_string()),
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

mod partition_key;
mod profile_record;
mod profiles_wal_topic;
mod wal_flag;
mod wal_function;
mod wal_location;
mod wal_mapping;
mod wal_sample;
mod wal_symbol_set;

pub use partition_key::partition_key;
pub use profile_record::ProfileRecord;
pub use profiles_wal_topic::PROFILES_WAL_TOPIC;
pub use wal_flag::WalFlag;
pub use wal_function::WalFunction;
pub use wal_location::WalLocation;
pub use wal_mapping::WalMapping;
pub use wal_sample::WalSample;
pub use wal_symbol_set::WalSymbolSet;
