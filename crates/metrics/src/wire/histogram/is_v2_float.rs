use super::pb;

pub(crate) fn is_v2_float(histogram: &pb::v2::Histogram) -> bool {
    matches!(
        histogram.count,
        Some(pb::v2::histogram::Count::CountFloat(_))
    ) || matches!(
        histogram.zero_count,
        Some(pb::v2::histogram::ZeroCount::ZeroCountFloat(_))
    ) || !histogram.positive_counts.is_empty()
        || !histogram.negative_counts.is_empty()
}
