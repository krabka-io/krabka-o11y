use super::{SeriesAgg, decimal_i64_to_f64, decimal_usize_to_f64};

#[must_use]
pub fn fold_bucket(agg: SeriesAgg, values: &[i64]) -> f64 {
    let sum: i64 = values.iter().sum();
    match agg {
        SeriesAgg::Sum => decimal_i64_to_f64(sum),
        SeriesAgg::Average => decimal_i64_to_f64(sum) / decimal_usize_to_f64(values.len()),
    }
}
