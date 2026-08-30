use super::*;

pub(crate) fn parse_span_selectors(selectors: &[String]) -> Result<Vec<u64>, ProfileError> {
    selectors
        .iter()
        .map(|selector| {
            let trimmed = selector.trim();
            trimmed
                .parse::<u64>()
                .or_else(|_| u64::from_str_radix(trimmed.strip_prefix("0x").unwrap_or(trimmed), 16))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ProfileError::Plan(format!("invalid span_selector: {err}")))
}
