use super::{DecodedSeries, Limits, PushError, TimeExt, sample_timestamp_bounds};

pub(crate) fn enforce_creation_grace_period(
    limits: &Limits,
    series: &[DecodedSeries],
    now_ms: i64,
) -> Result<(), PushError> {
    let newest_allowed_ms = now_ms.saturating_add(limits.creation_grace_period.millis_i64());
    if let Some(timestamp_ms) = series
        .iter()
        .filter_map(sample_timestamp_bounds)
        .map(|(_, max_timestamp)| max_timestamp)
        .max()
        .filter(|timestamp| *timestamp > newest_allowed_ms)
    {
        return Err(PushError::TooFarInFuture {
            timestamp_ms,
            newest_allowed_ms,
        });
    }
    Ok(())
}
