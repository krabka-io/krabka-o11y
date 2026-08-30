use super::*;

pub(crate) fn remote_read_histogram_zero_count(hist: &NativeHistogram) -> pb::v1::histogram::ZeroCount {
    if hist.is_float {
        pb::v1::histogram::ZeroCount::ZeroCountFloat(hist.zero_count)
    } else {
        pb::v1::histogram::ZeroCount::ZeroCountInt(hist.zero_count.to_u64().unwrap_or(u64::MAX))
    }
}
