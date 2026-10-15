use super::ProfileError;

pub(crate) fn parse_span_selectors(selectors: &[String]) -> Result<Vec<u64>, ProfileError> {
    selectors
        .iter()
        .map(|selector| {
            if selector.len() != 16 {
                return Err(ProfileError::Plan(format!(
                    "invalid span id length: {selector:?}"
                )));
            }
            u64::from_str_radix(selector, 16)
                .map_err(|err| ProfileError::Plan(format!("invalid span_selector: {err}")))
        })
        .collect()
}

pub(crate) fn parse_trace_selectors(selectors: &[String]) -> Result<Vec<Vec<u8>>, ProfileError> {
    selectors
        .iter()
        .map(|selector| {
            if selector.len() != 32 {
                return Err(ProfileError::Plan(
                    "trace_id_selector must contain 32 hexadecimal characters".to_string(),
                ));
            }
            hex::decode(selector)
                .map_err(|err| ProfileError::Plan(format!("invalid trace_id_selector: {err}")))
        })
        .collect()
}

pub(crate) fn stack_trace_call_sites(
    selector: Option<&super::pb::types::v1::StackTraceSelector>,
) -> Vec<String> {
    selector
        .filter(|selector| selector.go_pgo.is_none())
        .map_or_else(Default::default, |selector| {
            selector
                .call_site
                .iter()
                .map(|location| location.name.clone())
                .collect()
        })
}
