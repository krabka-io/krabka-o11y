use super::{ToPrimitive, pb};

pub(crate) fn v1_zero_count(histogram: &pb::v1::Histogram) -> f64 {
    use pb::v1::histogram::ZeroCount;

    match histogram.zero_count {
        Some(ZeroCount::ZeroCountInt(value)) => value.to_f64().unwrap_or(f64::MAX),
        Some(ZeroCount::ZeroCountFloat(value)) => value,
        None => 0.0,
    }
}
