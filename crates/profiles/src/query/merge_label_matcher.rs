use super::{LABEL_PROFILE_TYPE, ProfileError, label_matcher_value_escape, parse_label_selector};

pub(crate) fn merge_label_matcher(
    label_selector: &str,
    matcher: &str,
) -> Result<String, ProfileError> {
    let trimmed = label_selector.trim();
    let merged = if trimmed.is_empty() || trimmed == "{}" {
        format!("{{{matcher}}}")
    } else if let Some(inner) = trimmed.strip_prefix('{') {
        let inner = inner
            .strip_suffix('}')
            .ok_or_else(|| ProfileError::Plan("unclosed label selector".to_string()))?
            .trim();
        if inner.is_empty() {
            format!("{{{matcher}}}")
        } else {
            format!("{{{inner},{matcher}}}")
        }
    } else {
        format!("{{{trimmed},{matcher}}}")
    };

    parse_label_selector(&merged)?;
    Ok(merged)
}

pub(crate) fn merge_profile_type_selector(
    label_selector: &str,
    profile_type: &str,
) -> Result<String, ProfileError> {
    merge_label_matcher(
        label_selector,
        &format!(
            r#"{LABEL_PROFILE_TYPE}="{}""#,
            label_matcher_value_escape(profile_type)
        ),
    )
}
