use std::fmt;

/// A rule template either failed execution or needs an asynchronous query result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemplateRenderError {
    /// Resolve this query at the rule evaluation timestamp before rendering again.
    NeedsQuery(String),
    /// The upstream template execution contract rejected an operation.
    Execution(String),
}

impl fmt::Display for TemplateRenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NeedsQuery(query) => write!(formatter, "query result unavailable: {query}"),
            Self::Execution(error) => formatter.write_str(error),
        }
    }
}
impl std::error::Error for TemplateRenderError {}
