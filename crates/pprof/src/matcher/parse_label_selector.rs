use super::{
    LabelMatcher, MatchOp, ProfileError, Regex, split_top_level_commas, trim_selector,
    unescape_quoted,
};

/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
/// # Panics
/// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
pub fn parse_label_selector(input: &str) -> Result<Vec<LabelMatcher>, ProfileError> {
    let body = trim_selector(input)?;
    if body.trim().is_empty() {
        return Ok(Vec::new());
    }

    let matcher_re =
        Regex::new(r#"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"\s*$"#)
            .map_err(|err| ProfileError::Plan(format!("matcher regex failed to compile: {err}")))?;

    let mut out = Vec::new();
    for part in split_top_level_commas(body)? {
        let captures = matcher_re
            .captures(part)
            .ok_or_else(|| ProfileError::Plan(format!("invalid label matcher {part:?}")))?;
        let name = captures.get(1).expect("capture").as_str().to_string();
        let op = match captures.get(2).expect("capture").as_str() {
            "=" => MatchOp::Eq,
            "!=" => MatchOp::Neq,
            "=~" => MatchOp::Re,
            "!~" => MatchOp::Nre,
            other => {
                return Err(ProfileError::Plan(format!(
                    "invalid matcher operator {other:?}"
                )));
            }
        };
        let value = unescape_quoted(captures.get(3).expect("capture").as_str())?;
        out.push(LabelMatcher::new(name, op, value));
    }
    Ok(out)
}
