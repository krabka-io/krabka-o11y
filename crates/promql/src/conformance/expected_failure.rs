/// Expected query failure in the upstream test DSL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpectedFailure {
    /// An empty string accepts any failure; otherwise require this message.
    Message(String),
    /// Require an error matching the upstream `expect fail regex:` directive.
    Regex(String),
}
