use super::{TimeBounds, Uri, optional_seconds_param};

pub(crate) fn optional_time_bounds(uri: &Uri) -> Result<TimeBounds, String> {
    let start_ns = optional_seconds_param(uri, "start")?.unwrap_or(0);
    let end_ns = optional_seconds_param(uri, "end")?.unwrap_or(i64::MAX);
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok(TimeBounds { start_ns, end_ns })
}
