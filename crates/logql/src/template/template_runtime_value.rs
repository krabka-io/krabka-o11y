use super::{template_json_value_to_string, template_string_truthy, template_json_value_truthy};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TemplateRuntimeValue {
    String(String),
    Integer(i64),
    Json(serde_json::Value),
}

impl TemplateRuntimeValue {
    pub(crate) fn into_rendered_string(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Integer(value) => value.to_string(),
            Self::Json(value) => template_json_value_to_string(&value),
        }
    }

    pub(crate) fn as_rendered_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
            Self::Json(value) => template_json_value_to_string(value),
        }
    }

    pub(crate) fn is_template_string(&self) -> bool {
        matches!(
            self,
            Self::String(_) | Self::Json(serde_json::Value::String(_))
        )
    }

    pub(crate) fn is_truthy(&self) -> bool {
        match self {
            Self::String(value) => template_string_truthy(value),
            Self::Integer(value) => *value != 0,
            Self::Json(value) => template_json_value_truthy(value),
        }
    }
}
