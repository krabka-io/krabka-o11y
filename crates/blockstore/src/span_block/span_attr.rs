use super::AttrValue;

/// One generic span attribute.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpanAttr {
    pub key: String,
    pub is_array: bool,
    pub value: AttrValue,
}

impl SpanAttr {
    /// Creates a string-valued attribute for a span row.
    #[must_use]
    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            is_array: false,
            value: AttrValue::Str(vec![value.into()]),
        }
    }
}
