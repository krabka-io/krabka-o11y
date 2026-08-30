use super::*;

/// The `TokenBucket` kernel in `krabka-broker` is Creusot-verified over
/// integers. This function therefore converts the configured `Frequency` to
/// whole tokens per second.
pub(crate) fn rate_tokens_per_sec(limits: &Limits) -> u64 {
    let rate = limits
        .ingestion_rate
        .per_sec_f64()
        .ceil()
        .max(1.0)
        .to_u64()
        .unwrap_or(u64::MAX);
    if limits.ingestion_burst_profiles > 0 {
        rate.min(limits.ingestion_burst_profiles)
    } else {
        rate
    }
}
