use super::*;

pub(crate) fn prepare_matchers(matchers: &[LabelMatcher]) -> Result<Vec<PreparedMatcher>> {
    matchers.iter().map(PreparedMatcher::new).collect()
}
