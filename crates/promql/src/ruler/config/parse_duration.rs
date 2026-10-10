use super::{PromqlError, Time, TimeExt};
use crate::duration_terms::duration_terms_ms;

/// Parses a Prometheus duration string into a time extent.
///
/// This function supports the full Prometheus unit set (`ms`, `s`, `m`, `h`,
/// `d`, `w`, `y`) and compound durations such as `1h30m`. It matches the
/// conformance harness' `parse_duration_ms`. Empty, negative, or unparseable
/// input is a hard error.
pub(crate) fn parse_duration(duration: &str) -> Result<Time, PromqlError> {
    let src = duration.trim();
    if src.is_empty() {
        return Err(PromqlError::Exec("empty duration".into()));
    }
    if src == "0" {
        return Ok(Time::ZERO);
    }
    if src.starts_with('-') {
        return Err(PromqlError::Exec(format!("negative duration `{src}`")));
    }

    duration_terms_ms(src)
        .map(Time::from_millis)
        .map_err(PromqlError::Exec)
}
