use super::Sum;

pub(crate) fn sum_metadata_type(sum: &Sum) -> &'static str {
    if sum.is_monotonic { "counter" } else { "gauge" }
}
