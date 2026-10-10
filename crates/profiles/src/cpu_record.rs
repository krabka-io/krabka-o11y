//! A `process_cpu` profile record with one sample and one frame.
//!
//! Shared by the crate's unit tests and, through `#[path]`, by suites under
//! `tests/`, so it names the WAL types through its parent module, which
//! imports them from wherever its crate sees them.

use super::{ProfileRecord, WalSample, WalSymbolSet};

/// The profile type of every `process_cpu` record the tests write.
pub const CPU_PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

/// One `process_cpu` record with a single sample: `value` at `timestamp_ns`
/// on `stack`, in `service`'s series of `tenant`, whose one location resolves
/// to `function`.
pub struct CpuRecord<'a> {
    pub tenant: &'a str,
    pub service: &'a str,
    pub stack: Vec<u32>,
    pub value: i64,
    pub timestamp_ns: i64,
    pub function: &'a str,
}

pub fn cpu_record(record: CpuRecord<'_>) -> ProfileRecord {
    let CpuRecord {
        tenant,
        service,
        stack,
        value,
        timestamp_ns,
        function,
    } = record;
    ProfileRecord {
        tenant: tenant.to_string(),
        labels: vec![
            ("__name__".to_string(), "process_cpu".to_string()),
            ("__profile_type__".to_string(), CPU_PROFILE_TYPE.to_string()),
            ("service_name".to_string(), service.to_string()),
        ],
        profile_type: CPU_PROFILE_TYPE.to_string(),
        samples: vec![WalSample {
            stacktrace_location_refs: stack,
            value,
            timestamp_ns,
            span_id: None,
            trace_id: None,
        }],
        symbols: WalSymbolSet::single_frame(function.to_string()),
    }
}
