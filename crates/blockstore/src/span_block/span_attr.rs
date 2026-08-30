use super::*;

/// One generic span attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanAttr {
    pub key: String,
    pub is_array: bool,
    pub value: AttrValue,
}
