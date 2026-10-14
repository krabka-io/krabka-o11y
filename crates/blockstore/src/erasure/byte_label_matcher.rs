use crate::{BlockStoreError, MatchOp, Result};

/// A metric erasure matcher retaining the original Go string bytes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ByteLabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: Vec<u8>,
}

impl ByteLabelMatcher {
    #[must_use]
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }

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

/// Converts matcher groups into byte-preserving matcher groups.
pub fn map_byte_matcher_sets<S, M>(
    sets: impl IntoIterator<Item = S>,
    mut convert: impl FnMut(M) -> ByteLabelMatcher,
) -> Vec<Vec<ByteLabelMatcher>>
where
    S: IntoIterator<Item = M>,
{
    sets.into_iter()
        .map(|set| set.into_iter().map(&mut convert).collect())
        .collect()
}
