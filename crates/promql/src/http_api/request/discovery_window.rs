use super::*;

pub(crate) struct DiscoveryWindow {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
}

pub(crate) fn discovery_window(params: &DiscoveryParams) -> Result<DiscoveryWindow, ApiError> {
    let start_ms = match params.start.as_deref() {
        Some(start) => timestamp_ms(start)?,
        None => 0,
    };
    let end_ms = match params.end.as_deref() {
        Some(end) => timestamp_ms(end)?,
        None => i64::MAX,
    };
    validate_timestamp_range(start_ms, end_ms)?;
    Ok(DiscoveryWindow { start_ms, end_ms })
}
