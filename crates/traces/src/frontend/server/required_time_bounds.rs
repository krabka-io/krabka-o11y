use super::*;

pub(crate) fn required_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = required_seconds(uri, "start")?;
    let end_ns = required_seconds(uri, "end")?;
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}
