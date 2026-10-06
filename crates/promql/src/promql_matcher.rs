use krabka_blockstore::MatchOp;

use crate::PromqlString;

/// A metric matcher whose value retains Go string bytes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PromqlMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: PromqlString,
}

impl PromqlMatcher {
    #[must_use]
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<PromqlString>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }
    /// Converts a UTF-8 matcher for the shared posting index.
    #[must_use]
    pub fn index_matcher(&self) -> Option<krabka_blockstore::LabelMatcher> {
        Some(krabka_blockstore::LabelMatcher::new(
            &self.name,
            self.op,
            self.value.utf8()?,
        ))
    }
}
impl From<krabka_blockstore::LabelMatcher> for PromqlMatcher {
    fn from(value: krabka_blockstore::LabelMatcher) -> Self {
        Self::new(value.name, value.op, value.value)
    }
}
