//! A small, fully symbolized CPU profile for suites that push one and read it
//! back.
//!
//! Shared by this binary's suites and, through `#[path]`, by suites under
//! `tests/`, so it names only external crates.

use krabka_pprof::{PprofProfile, proto};

/// The caller frame of both samples.
pub const FUNC_WORK: &str = "main.work";
/// The leaf frame of the first sample.
pub const FUNC_HOT: &str = "main.hotloop";

/// A two-sample CPU profile: `main.hotloop` called from `main.work`, and
/// `main.work` on its own.
pub struct SyntheticCpuProfile {
    pub time_nanos: i64,
    /// The value of the `main.hotloop` <- `main.work` sample.
    pub hot_value: i64,
    /// The value of the `main.work` sample.
    pub work_value: i64,
}

impl SyntheticCpuProfile {
    /// The profile, encoded as pprof.
    pub fn encode(&self) -> Vec<u8> {
        let Self {
            time_nanos,
            hot_value,
            work_value,
        } = *self;
        // string_table: 0="" 1="cpu" 2="nanoseconds" 3=main.work 4=main.hotloop 5="app.go"
        let profile = proto::Profile {
            sample_type: vec![proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![
                proto::Sample {
                    location_id: vec![2, 1], // leaf-first: main.hotloop -> main.work
                    value: vec![hot_value],
                    label: Vec::new(),
                },
                proto::Sample {
                    location_id: vec![1], // main.work
                    value: vec![work_value],
                    label: Vec::new(),
                },
            ],
            mapping: vec![proto::Mapping {
                id: 1,
                symbolization: proto::MappingSymbolization::from_parts((true, false, false, false)),
                ..Default::default()
            }],
            location: vec![
                proto::Location {
                    id: 1,
                    mapping_id: 1,
                    address: 0x1000,
                    line: vec![proto::Line {
                        function_id: 1,
                        line: 10,
                        column: 0,
                    }],
                    is_folded: false,
                },
                proto::Location {
                    id: 2,
                    mapping_id: 1,
                    address: 0x2000,
                    line: vec![proto::Line {
                        function_id: 2,
                        line: 20,
                        column: 0,
                    }],
                    is_folded: false,
                },
            ],
            function: vec![
                proto::Function {
                    id: 1,
                    name: 3,
                    system_name: 3,
                    filename: 5,
                    start_line: 1,
                },
                proto::Function {
                    id: 2,
                    name: 4,
                    system_name: 4,
                    filename: 5,
                    start_line: 2,
                },
            ],
            string_table: vec![
                String::new(),
                "cpu".to_string(),
                "nanoseconds".to_string(),
                FUNC_WORK.to_string(),
                FUNC_HOT.to_string(),
                "app.go".to_string(),
            ],
            time_nanos,
            duration_nanos: 1_000_000_000,
            period_type: Some(proto::ValueType { r#type: 1, unit: 2 }),
            period: 10_000_000,
            ..Default::default()
        };
        PprofProfile::from(profile).encode()
    }
}
