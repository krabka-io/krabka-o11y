/// What a downsampled sample is keyed by once its timestamp has been rounded.
///
/// The field order is the block's declared sort key -- fingerprint, profile
/// type, timestamp -- followed by what distinguishes samples within one
/// bucket. A [`BTreeMap`](std::collections::BTreeMap) keyed by this therefore
/// yields rows already in the order the block writer wants them, which is what
/// lets a downsampling compaction stream its output instead of sorting it at
/// the end.
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
