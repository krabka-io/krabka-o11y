#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JsonPathPart {
    Field(String),
    Index(usize),
}
