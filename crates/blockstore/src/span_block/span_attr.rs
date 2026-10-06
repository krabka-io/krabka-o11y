use super::AttrValue;

/// One generic span attribute.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpanAttr {
    pub key: String,
    pub is_array: bool,
    pub value: AttrValue,
}
