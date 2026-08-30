use super::*;

/// `usize::MAX` is the CLI's "no limit" spelling. A zero rate is how the shared
/// limits express unlimited.
pub(crate) fn ingest_rate_from_cli(spans_per_sec: usize) -> Frequency {
    if spans_per_sec == usize::MAX {
        <Frequency as FrequencyExt>::ZERO
    } else {
        Frequency::from_per_sec(f64_from_usize(spans_per_sec))
    }
}
