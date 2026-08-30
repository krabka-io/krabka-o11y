use super::*;

/// `decode_form_component` decodes one `application/x-www-form-urlencoded`
/// field: `+` is a space, `%XX` is a byte, and everything else is itself.
/// A truncated or malformed escape is an error rather than a literal `%`,
/// and the decoded bytes still have to be UTF-8 -- a valid escape can name
/// a byte that is not.
#[test]
pub(crate) fn a_form_component_decodes_its_escapes_or_refuses_them() {
    let decode = |value: &str| super::super::prelude::decode_form_component(value).ok();

    check!(decode("plain") == Some("plain".to_string()));
    check!(decode("") == Some(String::new()));
    check!(decode("a+b") == Some("a b".to_string()), "plus is a space");
    check!(decode("a%20b") == Some("a b".to_string()), "and so is %20");
    check!(decode("%2F") == Some("/".to_string()));
    check!(
        decode("%2f") == Some("/".to_string()),
        "hex is case-insensitive"
    );
    check!(
        decode("%C3%A9") == Some("\u{e9}".to_string()),
        "a multi-byte character"
    );

    // A `%` that does not introduce two hex digits is an error, not a
    // literal percent sign -- at the end of the string and mid-string.
    check!(decode("a%").is_none());
    check!(decode("a%2").is_none());
    check!(decode("a%ZZb").is_none());
    check!(decode("100%").is_none());

    // A well-formed escape naming a byte that is not valid UTF-8.
    check!(decode("%FF").is_none());
}
