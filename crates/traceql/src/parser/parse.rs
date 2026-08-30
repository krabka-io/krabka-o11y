use super::*;

/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn parse(query: &str) -> Result<Query> {
    Parser {
        tokens: lex(query)?,
        pos: 0,
    }
    .parse_query()
}
