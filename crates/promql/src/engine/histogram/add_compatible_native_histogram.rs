use super::{
    NativeHistogram, PromqlError, Result, add_custom_histogram, add_exponential_histogram,
    combined_reset_hint,
};

pub(crate) fn add_compatible_native_histogram(
    left: &mut NativeHistogram,
    right: &NativeHistogram,
) -> Result<()> {
    if left.is_nhcb() != right.is_nhcb() {
        return Err(PromqlError::Unsupported(
            "cannot combine exponential and custom-bucket native histograms".to_string(),
        ));
    }

    left.reset_hint = combined_reset_hint(left.reset_hint, right.reset_hint);
    left.is_float |= right.is_float;
    left.count += right.count;
    left.sum += right.sum;
    if left.is_nhcb() {
        add_custom_histogram(left, right);
    } else {
        add_exponential_histogram(left, right);
    }
    left.start_timestamp_ms = match left.start_timestamp_ms {
        Some(left_start) => match right.start_timestamp_ms {
            Some(right_start) => Some(left_start.min(right_start)),
            None => Some(left_start),
        },
        None => right.start_timestamp_ms,
    };
    Ok(())
}
