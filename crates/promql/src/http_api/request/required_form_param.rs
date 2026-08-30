use super::ApiError;

pub(crate) fn required_form_param(value: Option<String>, name: &str) -> Result<String, ApiError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_data(format!("missing {name} parameter")))
}
