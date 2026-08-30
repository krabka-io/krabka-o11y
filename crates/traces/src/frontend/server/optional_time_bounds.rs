use super::*;

pub(crate) fn optional_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = optional_seconds(uri, "start")?.unwrap_or(0);
    let end_ns = optional_seconds(uri, "end")?.unwrap_or(i64::MAX);
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}
