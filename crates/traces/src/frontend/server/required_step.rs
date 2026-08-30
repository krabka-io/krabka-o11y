use super::{Uri, query_param, parse_step_to_ns};

pub(crate) fn required_step(uri: &Uri) -> Result<i64, String> {
    let Some(value) = query_param(uri, "step") else {
        return Err("missing query parameter step".to_string());
    };
    let step = parse_step_to_ns(&value).ok_or("invalid step")?;
    if step <= 0 {
        return Err("step must be positive".to_string());
    }
    Ok(step)
}
