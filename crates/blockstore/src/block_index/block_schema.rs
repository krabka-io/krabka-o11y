use super::RequiredColumn;

/// A signal's declared block schema and sort key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSchema {
    pub required: Vec<RequiredColumn>,
    pub sort_key: Vec<String>,
}
