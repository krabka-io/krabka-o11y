use super::{ApiError, LabelMatcher, MatchOp, pb};

pub(crate) fn remote_read_matchers(
    matchers: &[pb::v1::LabelMatcher],
) -> Result<Vec<LabelMatcher>, ApiError> {
    matchers
        .iter()
        .map(|matcher| {
            let op = MatchOp::try_from(matcher.r#type).map_err(|other| {
                ApiError::bad_data(format!("unknown remote_read matcher type {other}"))
            })?;
            Ok(LabelMatcher::new(
                &matcher.name,
                op,
                crate::PromqlString::from(matcher.value.clone()),
            ))
        })
        .collect()
}
