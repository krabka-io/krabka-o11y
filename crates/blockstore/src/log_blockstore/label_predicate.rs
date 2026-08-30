use super::*;

#[derive(Clone, Debug)]
pub struct LabelPredicate {
    pub(crate) name: String,
    pub(crate) op: MatchOp,
    pub(crate) value: String,
    pub(crate) regex: Option<Regex>,
}

impl LabelPredicate {
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn new(
        name: impl Into<String>,
        op: MatchOp,
        value: impl Into<String>,
    ) -> Result<Self, BlockStoreError> {
        let value = value.into();
        let regex = if matches!(op, MatchOp::RegexEqual | MatchOp::RegexNotEqual) {
            Some(
                Regex::new(&anchored_regex_pattern(&value)).map_err(|source| {
                    BlockStoreError::InvalidRegex {
                        pattern: value.clone(),
                        source,
                    }
                })?,
            )
        } else {
            None
        };
        Ok(Self {
            name: name.into(),
            op,
            value,
            regex,
        })
    }

    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        let candidate = labels.get(&self.name);
        match self.op {
            MatchOp::Equal => candidate == Some(&self.value),
            MatchOp::NotEqual => candidate != Some(&self.value),
            MatchOp::RegexEqual => self.regex_matches(candidate.map_or("", String::as_str)),
            MatchOp::RegexNotEqual => candidate.is_none_or(|value| !self.regex_matches(value)),
        }
    }

    pub(crate) fn exact_posting_key(&self) -> Option<(&str, &str)> {
        (self.op == MatchOp::Equal).then_some((&self.name, &self.value))
    }

    pub(crate) fn regex_matches(&self, value: &str) -> bool {
        self.regex
            .as_ref()
            .expect("regex predicate validated at construction")
            .is_match(value)
    }
}
