use super::*;

pub(crate) fn v2_zero_count(histogram: &pb::v2::Histogram) -> f64 {
    use pb::v2::histogram::ZeroCount;

    match histogram.zero_count {
        Some(ZeroCount::ZeroCountInt(value)) => value.to_f64().unwrap_or(f64::MAX),
        Some(ZeroCount::ZeroCountFloat(value)) => value,
        None => 0.0,
    }
}
