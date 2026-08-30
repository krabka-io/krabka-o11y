use super::{DataType, PromotedSpanAttrType, SCOL_PROMOTED_ATTR_PREFIX};

/// A configured attribute column promoted out of the generic attribute lists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromotedSpanAttr {
    pub key: String,
    pub value_type: PromotedSpanAttrType,
}

impl PromotedSpanAttr {
    #[must_use]
    pub fn string(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::String,
        }
    }

    #[must_use]
    pub fn int(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::Int,
        }
    }

    #[must_use]
    pub fn double(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::Double,
        }
    }

    #[must_use]
    pub fn bool(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::Bool,
        }
    }

    #[must_use]
    pub fn column_name(&self) -> String {
        format!("{SCOL_PROMOTED_ATTR_PREFIX}{}", self.key)
    }

    #[must_use]
    pub fn data_type(&self) -> DataType {
        self.value_type.data_type()
    }
}
