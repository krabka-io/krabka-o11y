use super::{LABEL_PROFILE_TYPE, ProfileError, label_matcher_value_escape, merge_label_matcher};

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
