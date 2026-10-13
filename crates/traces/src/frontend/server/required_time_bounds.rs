use super::{Uri, required_seconds};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RequiredTimeBounds {
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

pub(crate) fn required_time_bounds(uri: &Uri) -> Result<RequiredTimeBounds, String> {
    let start_ns = required_seconds(uri, "start")?;
    let end_ns = required_seconds(uri, "end")?;
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok(RequiredTimeBounds { start_ns, end_ns })
}
