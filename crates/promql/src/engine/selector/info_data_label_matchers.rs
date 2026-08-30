use super::*;

pub(crate) fn info_data_label_matchers(selector: &VectorSelector) -> Result<Vec<LabelMatcher>> {
    let matcher_sets = label_matcher_sets(selector);
    let [matchers] = matcher_sets.as_slice() else {
        return Err(PromqlError::Plan(
            "info data label selector does not support or matchers".to_string(),
        ));
    };
    Ok(matchers.clone())
}
