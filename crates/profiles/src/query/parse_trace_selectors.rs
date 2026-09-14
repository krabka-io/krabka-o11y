use super::ProfileError;

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
