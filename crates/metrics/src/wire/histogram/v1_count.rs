use super::{ToPrimitive, pb};

pub(crate) fn v1_count(histogram: &pb::v1::Histogram) -> f64 {
    use pb::v1::histogram::Count;

    match histogram.count {
        Some(Count::CountInt(value)) => value.to_f64().unwrap_or(f64::MAX),
        Some(Count::CountFloat(value)) => value,
        None => 0.0,
    }
}
