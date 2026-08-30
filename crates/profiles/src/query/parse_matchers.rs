use super::*;

pub(crate) fn parse_matchers(
    matchers: &[String],
) -> Result<Vec<krabka_blockstore::LabelMatcher>, ProfileError> {
    let mut out = Vec::new();
    for matcher in matchers {
        out.extend(parse_label_selector(matcher)?);
    }
    Ok(out)
}
