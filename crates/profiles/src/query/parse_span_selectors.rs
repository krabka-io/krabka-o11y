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
