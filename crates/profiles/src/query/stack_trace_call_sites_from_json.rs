use super::{ProfileError, StackTraceSelectorJson};

pub(crate) fn stack_trace_call_sites_from_json(
    selector: &str,
) -> Result<Vec<String>, ProfileError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Ok(Vec::new());
    }
    let selector: StackTraceSelectorJson = serde_json::from_str(selector)
        .map_err(|err| ProfileError::Plan(format!("invalid stack_trace_selector: {err}")))?;
    Ok(selector
        .call_site
        .into_iter()
        .map(|location| location.name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect())
}
