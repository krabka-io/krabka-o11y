use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum MatchValue {
    Str(String),
    /// A string operand with a cached regex compilation result.
    Regex(Arc<PreparedRegex>),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
}

/// A whole-string regex and its original string operand.
#[derive(Debug)]
pub struct PreparedRegex {
    pub(crate) pattern: String,
    pub(crate) compiled: Option<regex::Regex>,
}

impl PartialEq for PreparedRegex {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}

impl PreparedRegex {
    pub(crate) fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_owned(),
            compiled: regex::Regex::new(&format!("^(?:{pattern})$")).ok(),
        }
    }
}
