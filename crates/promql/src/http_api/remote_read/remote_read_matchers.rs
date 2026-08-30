use super::{pb, LabelMatcher, ApiError, MatchOp};

pub(crate) fn remote_read_matchers(matchers: &[pb::v1::LabelMatcher]) -> Result<Vec<LabelMatcher>, ApiError> {
    matchers
        .iter()
        .map(|matcher| {
            let op = match matcher.r#type {
                0 => MatchOp::Eq,
                1 => MatchOp::Neq,
                2 => MatchOp::Re,
                3 => MatchOp::Nre,
                other => {
                    return Err(ApiError::bad_data(format!(
                        "unknown remote_read matcher type {other}"
                    )));
                }
            };
            Ok(LabelMatcher::new(&matcher.name, op, &matcher.value))
        })
        .collect()
}
