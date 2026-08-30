use super::{
    NativeHistogram, OverTimeFn, add_compatible_native_histogram, scale_native_histogram_values,
};

pub(crate) fn over_time_histogram_sample(
    histograms: &[NativeHistogram],
    kind: OverTimeFn,
) -> Option<NativeHistogram> {
    let mut out = histograms.first()?.clone();
    for histogram in &histograms[1..] {
        add_compatible_native_histogram(&mut out, histogram).ok()?;
    }
    if matches!(kind, OverTimeFn::Avg) {
        let count: f64 = histograms.iter().map(|_| 1.0).sum();
        scale_native_histogram_values(&mut out, 1.0 / count);
    }
    Some(out)
}
