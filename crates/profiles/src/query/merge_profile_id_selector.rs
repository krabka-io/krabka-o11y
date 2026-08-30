use super::*;

pub(crate) fn merge_profile_id_selector(
    label_selector: &str,
    profile_ids: &[String],
) -> Result<String, ProfileError> {
    if profile_ids.is_empty() {
        return Ok(label_selector.to_string());
    }

    let matcher = if profile_ids.len() == 1 {
        format!(
            r#"{PROFILE_ID_LABEL}="{}""#,
            label_matcher_value_escape(&profile_ids[0])
        )
    } else {
        let regex = profile_ids
            .iter()
            .map(|value| regex::escape(value))
            .collect::<Vec<_>>()
            .join("|");
        format!(
            r#"{PROFILE_ID_LABEL}=~"^(?:{})$""#,
            label_matcher_value_escape(&regex)
        )
    };

    merge_label_matcher(label_selector, &matcher)
}
