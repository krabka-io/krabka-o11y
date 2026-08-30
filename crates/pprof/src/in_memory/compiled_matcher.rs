use super::{LabelMatcher, Regex};

pub(crate) enum CompiledMatcher<'a> {
    Literal(&'a LabelMatcher),
    Regex(&'a LabelMatcher, Regex),
}
