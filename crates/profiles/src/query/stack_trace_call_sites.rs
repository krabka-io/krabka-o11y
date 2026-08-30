use super::pb;

pub(crate) fn stack_trace_call_sites(
    selector: Option<&pb::types::v1::StackTraceSelector>,
) -> Vec<String> {
    selector
        .map(|selector| {
            selector
                .call_site
                .iter()
                .map(|location| location.name.trim())
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}
