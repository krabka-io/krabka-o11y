use super::{Line, Result, parse_error};
use crate::duration_terms::duration_terms_ms;

pub(crate) fn parse_duration_ms(src: &str, line: Line<'_>) -> Result<i64> {
    let src = src.trim();
    if src == "0" {
        return Ok(0);
    }
    duration_terms_ms(src).map_err(|message| parse_error(line, message))
}
