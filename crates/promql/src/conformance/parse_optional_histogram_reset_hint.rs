use super::{BTreeMap, Line, ResetHint, Result, parse_error};

pub(crate) fn parse_optional_histogram_reset_hint(
    fields: &BTreeMap<&str, &str>,
    line: Line<'_>,
) -> Result<ResetHint> {
    match fields.get("counter_reset_hint").copied() {
        None | Some("unknown") => Ok(ResetHint::Unknown),
        Some("reset") => Ok(ResetHint::Yes),
        Some("not_reset") => Ok(ResetHint::No),
        Some("gauge") => Ok(ResetHint::Gauge),
        Some(value) => Err(parse_error(
            line,
            format!("invalid histogram counter_reset_hint `{value}`"),
        )),
    }
}
