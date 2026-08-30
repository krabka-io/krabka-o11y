use super::{NativeHistogram, ToPrimitive, pb};

pub(crate) fn remote_read_histogram_count(hist: &NativeHistogram) -> pb::v1::histogram::Count {
    if hist.is_float {
        pb::v1::histogram::Count::CountFloat(hist.count)
    } else {
        pb::v1::histogram::Count::CountInt(hist.count.to_u64().unwrap_or(u64::MAX))
    }
}
