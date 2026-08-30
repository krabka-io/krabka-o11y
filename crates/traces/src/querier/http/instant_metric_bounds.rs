use super::{Uri, optional_seconds_param, query_param, required_seconds_param};

pub(crate) fn instant_metric_bounds(uri: &Uri) -> Result<(i64, i64, i64, i64), String> {
    if query_param(uri, "start").is_some() || query_param(uri, "end").is_some() {
        let start_ns = required_seconds_param(uri, "start")?;
        let end_ns = required_seconds_param(uri, "end")?;
        let step_ns = end_ns
            .checked_sub(start_ns)
            .and_then(|width| width.checked_add(1))
            .filter(|step| *step > 0)
            .ok_or_else(|| "end must be >= start".to_string())?;
        return Ok((start_ns, end_ns, step_ns, end_ns));
    }

    let ts_ns = optional_seconds_param(uri, "time")?.unwrap_or(0);
    Ok((ts_ns, ts_ns, 1_000_000_000, ts_ns))
}
