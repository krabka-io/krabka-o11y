/// Key for a downsampled sample after its timestamp has been rounded.
///
/// The fields begin with the block's sort key, then distinguish samples in a
/// bucket so a `BTreeMap` emits rows in writer order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DownsampleKey {
    pub(crate) series_fingerprint: u64,
    pub(crate) profile_type: String,
    pub(crate) timestamp: i64,
    pub(crate) stacktrace_id: u64,
    pub(crate) stacktrace_partition: u64,
    pub(crate) span_id: Option<u64>,
    pub(crate) trace_id: Option<Vec<u8>>,
}

/// Resolution policy for profile downsampling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DownsamplePolicy {
    pub resolution_ns: i64,
}
