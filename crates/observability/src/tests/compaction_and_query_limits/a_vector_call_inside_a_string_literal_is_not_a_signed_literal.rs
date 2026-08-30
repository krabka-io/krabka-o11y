use super::*;

/// `vector(` written inside a string literal is just text. Telling it from
/// a real call needs the scanner to track exactly where the literal ends:
/// a scanner that leaves early reads the text as a call, and one that
/// never leaves swallows the code that follows.
#[test]
pub(crate) fn a_vector_call_inside_a_string_literal_is_not_a_signed_literal() {
    for (query, reported) in [
        ("vector(-1)", true),
        ("vector( +2)", true),
        ("vector(1)", false),
        // The call sits one character into the literal, so a scanner that
        // leaves the literal early lands on it and reports it.
        (
            r#"label_replace(vector(1), "dst", "Xvector( -1)", "s", "")"#,
            false,
        ),
        // And a real call after a literal is still reached, which a
        // scanner that never leaves one would never see.
        (
            r#"label_replace(vector(1), "a", "b", "s", "") + vector(-1)"#,
            true,
        ),
    ] {
        check!(
            signed_vector_function_literal_error(query).is_some() == reported,
            "{query}: {:?}",
            signed_vector_function_literal_error(query)
        );
    }
}
