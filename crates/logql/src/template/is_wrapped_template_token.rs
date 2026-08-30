
pub(crate) fn is_wrapped_template_token(token: &str, quote: char) -> bool {
    if token.len() < quote.len_utf8().saturating_mul(2) {
        return false;
    }
    token.starts_with(quote) && token.ends_with(quote)
}
