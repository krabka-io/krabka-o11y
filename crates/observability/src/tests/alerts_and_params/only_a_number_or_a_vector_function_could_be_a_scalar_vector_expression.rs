use super::*;

/// `could_be_scalar_vector_expression` is the cheap gate two of the query
/// parsers run before doing real work. It admits anything starting like a
/// number or a parenthesis, and among identifiers ONLY the three functions
/// that can produce a vector -- so `sum(...)` is turned away here and
/// parsed elsewhere.
#[test]
pub(crate) fn only_a_number_or_a_vector_function_could_be_a_scalar_vector_expression() {
    let could_be = super::super::prelude::could_be_scalar_vector_expression;

    // Numbers and the characters a numeric expression can open with.
    check!(could_be("1"));
    check!(could_be("1+1"));
    check!(could_be("+1"));
    check!(could_be("-1"));
    check!(could_be(".5"));
    check!(could_be("(1+1)"));
    check!(could_be("  1"), "leading whitespace is trimmed");

    // The three vector-producing functions, and nothing else.
    check!(could_be("vector(1)"));
    check!(could_be("label_replace(vector(1),\"a\",\"b\",\"c\",\"d\")"));
    check!(could_be("label_join(vector(1),\"a\",\"b\")"));
    check!(
        !could_be("sum(rate(x[5m]))"),
        "an aggregation is parsed elsewhere"
    );
    check!(!could_be("up"));

    // The identifier must match WHOLE: a longer name starting with one of
    // the three is not one of them.
    check!(!could_be("vectorise(1)"));
    check!(!could_be("vector_total"));

    // Nothing, and things that start with neither.
    check!(!could_be(""));
    check!(!could_be("   "));
    check!(
        !could_be("{app=\"a\"}"),
        "a matcher is not a scalar expression"
    );
    check!(!could_be("\"quoted\""));
}
