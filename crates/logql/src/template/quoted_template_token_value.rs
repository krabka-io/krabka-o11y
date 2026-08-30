use super::{ParseError, decode_quoted_fragment, is_wrapped_template_token};

pub(crate) fn quoted_template_token_value(token: &str) -> Result<Option<String>, ParseError> {
    if is_wrapped_template_token(token, '`') {
        return Ok(Some(token[1..token.len() - 1].to_string()));
    }
    if is_wrapped_template_token(token, '"') {
        return Ok(Some(decode_quoted_fragment(&token[1..token.len() - 1])?));
    }
    Ok(None)
}
