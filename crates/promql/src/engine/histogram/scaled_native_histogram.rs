use super::*;

pub(crate) fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    scale_native_histogram_values(&mut out, factor);
    if factor.is_sign_negative() {
        out.reset_hint = ResetHint::Gauge;
    }
    out
}
