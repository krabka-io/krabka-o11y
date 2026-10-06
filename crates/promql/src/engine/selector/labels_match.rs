use super::{LabelMatcher, Labels, MatchOp, PromqlError, Regex, Result};

pub(crate) fn labels_match(labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool> {
    for matcher in matchers {
        let bytes = labels
            .get_value(&matcher.name)
            .map_or(&[][..], crate::PromqlString::as_bytes);
        let value = labels.get(&matcher.name).unwrap_or("");
        let is_match = match matcher.op {
            MatchOp::Eq => bytes == matcher.value.as_bytes(),
            MatchOp::Neq => bytes != matcher.value.as_bytes(),
            MatchOp::Re | MatchOp::Nre => {
                let regex = Regex::new(&format!("^(?s:{})$", matcher.value)).map_err(|error| {
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
