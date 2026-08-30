
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelSelectionMatcher {
    Equal(String),
    Regex(String),
}
