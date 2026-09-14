use super::NativeHistogram;
use crate::engine::add_compatible_native_histogram;

pub(crate) fn add_histogram_step(
    start: &NativeHistogram,
    step: &NativeHistogram,
    offset: u32,
) -> NativeHistogram {
    if offset == 0 {
        return start.clone();
    }
    let multiplier = f64::from(offset);
    let mut histogram = start.clone();
    let mut increment = step.clone();
    increment.sum *= multiplier;
    increment.count *= multiplier;
    increment.zero_count *= multiplier;
    for count in increment
        .positive_counts
        .iter_mut()
        .chain(&mut increment.negative_counts)
    {
        *count *= multiplier;
    }
    add_compatible_native_histogram(&mut histogram, &increment)
        .expect("histogram expansion operands have compatible bucket kinds");
    histogram
}
