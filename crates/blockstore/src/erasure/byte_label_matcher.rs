use crate::{BlockStoreError, MatchOp, Result};

/// A metric erasure matcher retaining the original Go string bytes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ByteLabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: Vec<u8>,
}

impl ByteLabelMatcher {
    /// Matches raw equality values and the Go rune view for regular expressions.
    ///
    /// # Errors
    /// Returns an error when a persisted regexp is not valid UTF-8 or syntax.
    pub fn matches(&self, raw_value: &[u8], rune_value: &str) -> Result<bool> {
        match self.op {
            MatchOp::Eq => Ok(raw_value == self.value),
            MatchOp::Neq => Ok(raw_value != self.value),
            MatchOp::Re | MatchOp::Nre => {
                let source = std::str::from_utf8(&self.value).map_err(|error| {
                    BlockStoreError::InvalidBlock(format!("invalid metric erasure regexp: {error}"))
                })?;
                let regexp = regex::Regex::new(&format!("^(?s:{source})$")).map_err(|error| {
                    BlockStoreError::InvalidBlock(format!("invalid metric erasure regexp: {error}"))
                })?;
                let matched = regexp.is_match(rune_value);
                Ok(if self.op == MatchOp::Re {
                    matched
                } else {
                    !matched
                })
            }
        }
    }
}
