
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineFilterOp {
    Contains,
    NotContains,
    Regex,
    NotRegex,
    Pattern,
    NotPattern,
}
