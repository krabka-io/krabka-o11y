use super::pb;

pub(crate) fn is_v1_float(histogram: &pb::v1::Histogram) -> bool {
    matches!(
        histogram.count,
        Some(pb::v1::histogram::Count::CountFloat(_))
    ) || matches!(
        histogram.zero_count,
        Some(pb::v1::histogram::ZeroCount::ZeroCountFloat(_))
    ) || !histogram.positive_counts.is_empty()
        || !histogram.negative_counts.is_empty()
}
