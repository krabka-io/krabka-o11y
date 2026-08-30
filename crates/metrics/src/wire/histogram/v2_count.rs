use super::*;

pub(crate) fn v2_count(histogram: &pb::v2::Histogram) -> f64 {
    use pb::v2::histogram::Count;

    match histogram.count {
        Some(Count::CountInt(value)) => value.to_f64().unwrap_or(f64::MAX),
        Some(Count::CountFloat(value)) => value,
        None => 0.0,
    }
}
