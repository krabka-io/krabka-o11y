
pub(crate) fn sign_is_unary_or_exponent(input: &str, at: usize) -> bool {
    let before_sign = input[..at].trim_end();
    let Some(last_char) = before_sign.chars().next_back() else {
        return true;
    };
    if matches!(
        last_char,
        '+' | '-' | '*' | '/' | '%' | '^' | '>' | '<' | '=' | '!'
    ) {
        return true;
    }
    if matches!(last_char, 'e' | 'E') {
        let mantissa = before_sign[..before_sign.len() - last_char.len_utf8()].trim_end();
        return mantissa
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_ascii_digit() || ch == '.');
    }
    false
}
