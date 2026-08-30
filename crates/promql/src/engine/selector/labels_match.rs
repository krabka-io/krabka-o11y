use super::{Labels, LabelMatcher, Result, MatchOp, Regex, PromqlError};

pub(crate) fn labels_match(labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool> {
    for matcher in matchers {
        let value = labels.get(&matcher.name).unwrap_or("");
        let is_match = match matcher.op {
            MatchOp::Eq => value == matcher.value,
            MatchOp::Neq => value != matcher.value,
            MatchOp::Re | MatchOp::Nre => {
                let regex = Regex::new(&format!("^(?:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?;
                let regex_matches = regex.is_match(value);
                if matcher.op == MatchOp::Re {
                    regex_matches
                } else {
                    !regex_matches
                }
            }
        };
        if !is_match {
            return Ok(false);
        }
    }
    Ok(true)
}
