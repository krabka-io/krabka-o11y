use super::*;

/// `log_pattern_token` masks the variable part of a log token so lines that
/// differ only in their ids collapse to one pattern. A `key=value` token
/// keeps its KEY and masks only the value, because the key is what makes
/// two lines the same kind of line.
#[test]
pub(crate) fn a_log_pattern_token_masks_only_its_variable_part() {
    let token = super::super::prelude::log_pattern_token;

    // A bare token is masked whole, or kept whole.
    check!(token("connected") == "connected", "a word is not variable");
    check!(token("12345") == "<_>", "a number is");
    check!(token("1.5") == "<_>");

    // A key=value token keeps the key and masks the value.
    check!(token("user_id=12345") == "user_id=<_>");
    check!(
        token("status=ok") == "status=ok",
        "a non-variable value is kept"
    );
    check!(
        token("id=550e8400-e29b-41d4-a716-446655440000") == "id=<_>",
        "a uuid is variable"
    );

    // Half a pair is not a pair: an empty key or value leaves the token
    // alone rather than producing "=<_>" or "<_>=".
    check!(token("=12345") == "=12345");
    check!(token("user_id=") == "user_id=");
    check!(token("=") == "=");

    // Only the FIRST equals splits, so a value containing one is masked
    // whole rather than re-split.
    check!(
        token("q=a=12345") == "q=a=12345",
        "the value is not variable"
    );
    check!(token("") == "");
}
