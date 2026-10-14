use super::*;

struct VectorCallCase {
    query: &'static str,
    reported: bool,
}

/// `vector(` written inside a string literal is just text. Telling it from
/// a real call needs the scanner to track exactly where the literal ends:
/// a scanner that leaves early reads the text as a call, and one that
/// never leaves swallows the code that follows.
#[yare::parameterized(
    signed_integer = { VectorCallCase { query: "vector(-1)", reported: true } },
    spaced_positive = { VectorCallCase { query: "vector( +2)", reported: true } },
    unsigned = { VectorCallCase { query: "vector(1)", reported: false } },
    text_inside_literal = { VectorCallCase { query: r#"label_replace(vector(1), "dst", "Xvector( -1)", "s", "")"#, reported: false } },
    call_after_literal = { VectorCallCase { query: r#"label_replace(vector(1), "a", "b", "s", "") + vector(-1)"#, reported: true } },
)]
pub(crate) fn a_vector_call_inside_a_string_literal_is_not_a_signed_literal(case: VectorCallCase) {
    check!(
        signed_vector_function_literal_error(case.query).is_some() == case.reported,
        "{}: {:?}",
        case.query,
        signed_vector_function_literal_error(case.query)
    );
}
