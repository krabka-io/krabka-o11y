use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct SpanMatcher {
    pub scope: MatchScope,
    pub key: String,
    pub op: MatchCmp,
    pub value: MatchValue,
    pub negated: bool,
}
