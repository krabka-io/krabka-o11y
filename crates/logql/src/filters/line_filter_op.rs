#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A `LogQL` line-filter operation.
pub enum LineFilterOp {
    Contains,
    NotContains,
    Regex,
    NotRegex,
    Pattern,
    NotPattern,
}
