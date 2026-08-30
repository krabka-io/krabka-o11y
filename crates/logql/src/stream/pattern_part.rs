use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PatternPart {
    Capture(String),
    Literal(String),
}
