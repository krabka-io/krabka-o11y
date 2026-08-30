
#[cfg(feature = "experimental-functions")]
pub(crate) fn limit_ratio_includes_sample(ratio: f64, labels: &Labels) -> bool {
    let sample_offset = prometheus_labels_hash(labels).to_f64().unwrap_or(f64::MAX)
        / u64::MAX.to_f64().unwrap_or(f64::MAX);
    if ratio == 0.0 {
        false
    } else if ratio.is_sign_positive() {
        sample_offset < ratio
    } else {
        sample_offset >= 1.0 + ratio
    }
}
