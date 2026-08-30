use super::HistogramDataPoint;

pub(crate) fn exemplar_belongs_to_bucket(
    value: f64,
    point: &HistogramDataPoint,
    bucket_idx: usize,
) -> bool {
    let lower_ok = bucket_idx
        .checked_sub(1)
        .and_then(|lower_idx| point.explicit_bounds.get(lower_idx))
        .is_none_or(|lower| value > *lower);
    let upper_ok = point
        .explicit_bounds
        .get(bucket_idx)
        .is_none_or(|upper| value <= *upper);
    lower_ok && upper_ok
}
