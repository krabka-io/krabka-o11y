use super::{Labels, MatchOp};

/// One label matcher's name, operator and raw value.
#[derive(Clone, Copy)]
pub(crate) struct LabelValueMatcher<'a> {
    pub(crate) name: &'a str,
    pub(crate) op: MatchOp,
    /// The value `=` and `!=` compare against, as raw bytes.
    pub(crate) expected: &'a [u8],
}

/// Matches the value `labels` holds for the `matcher` label.
///
/// `=` and `!=` compare the raw value bytes with the expected bytes, so a byte
/// label keeps its identity. `=~` and `!~` hand the UTF-8 value to
/// `regex_matches`, which only runs for those two operators. An absent label
/// matches as `""`.
pub(crate) fn label_value_matches<E>(
    labels: &Labels,
    matcher: LabelValueMatcher<'_>,
    regex_matches: impl FnOnce(&str) -> Result<bool, E>,
) -> Result<bool, E> {
    let LabelValueMatcher { name, op, expected } = matcher;
    let bytes = labels
        .get_value(name)
        .map_or(&[][..], crate::PromqlString::as_bytes);
    Ok(match op {
        MatchOp::Eq => bytes == expected,
        MatchOp::Neq => bytes != expected,
        MatchOp::Re | MatchOp::Nre => {
            let regex_matches = regex_matches(labels.get(name).unwrap_or(""))?;
            if op == MatchOp::Re {
                regex_matches
            } else {
                !regex_matches
            }
        }
    })
}
