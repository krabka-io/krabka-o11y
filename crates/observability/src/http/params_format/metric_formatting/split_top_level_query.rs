use crate::http::params_format::QuotedTextScanner;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopLevelQuerySplit<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'static str,
    pub(crate) right: &'a str,
}

/// Splits `query` at the first operator that sits outside every quote,
/// parenthesis, bracket and brace, and returns the left side, the operator,
/// and the right side with its leading whitespace trimmed.
///
/// `operator_at` names the operator that starts at a top-level byte index,
/// if one does; it never sees a quote or a bracket character.
pub(crate) fn split_top_level_query(
    query: &str,
    operator_at: impl Fn(usize, char) -> Option<&'static str>,
) -> Option<TopLevelQuerySplit<'_>> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quotes = QuotedTextScanner::default();
    for character in query.char_indices() {
        let index = character.0;
        let ch = character.1;
        if quotes.in_quotes(ch) {
            continue;
        }
        match ch {
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            _ if parens == 0 && brackets == 0 && braces == 0 => {
                if let Some(operator) = operator_at(index, ch) {
                    return Some(TopLevelQuerySplit {
                        left: &query[..index],
                        operator,
                        right: query[index + operator.len()..].trim_start(),
                    });
                }
            }
            _ => {}
        }
    }
    None
}
