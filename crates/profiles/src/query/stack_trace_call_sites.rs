use super::pb;

pub(crate) fn stack_trace_call_sites(
    selector: Option<&pb::types::v1::StackTraceSelector>,
) -> Vec<String> {
    selector
        .filter(|selector| selector.go_pgo.is_none())
        .map_or_default(|selector| {
            selector
                .call_site
                .iter()
                .map(|location| location.name.clone())
                .collect()
        })
}
