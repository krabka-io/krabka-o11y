use super::{
    LabelMatcher, LabelValueMatcher, Labels, PromqlError, Regex, Result, label_value_matches,
};

pub(crate) fn labels_match(labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool> {
    for matcher in matchers {
        let is_match = label_value_matches::<PromqlError>(
            labels,
            LabelValueMatcher {
                name: &matcher.name,
                op: matcher.op,
                expected: matcher.value.as_bytes(),
            },
            |label_value| {
                let regex = Regex::new(&format!("^(?s:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?;
                Ok(regex.is_match(label_value))
            },
        )?;
        if !is_match {
            return Ok(false);
        }
    }
    Ok(true)
}
