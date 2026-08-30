use super::*;

/// `parse_metric_vector_comparison_expression` recognises a comparison
/// between a metric query and a `vector(...)` literal, and records WHICH
/// side the literal was on -- the two are not interchangeable, since
/// `up > vector(1)` and `vector(1) > up` select opposite samples.
///
/// Exactly one side must be a vector literal. Two of them, or none, is not
/// this kind of expression and is refused rather than guessed at, so both
/// rejections are checked as well as both acceptances.
#[test]
pub(crate) fn a_vector_comparison_records_which_side_the_literal_was_on() {
    use krabka_logql::ComparisonOp;

    let parse = super::super::prelude::parse_metric_vector_comparison_expression;

    let right = parse("up > vector(1)").expect("a vector on the right");
    check!(right.metric_query == "up");
    check!(right.vector_query == "vector(1)");
    check!(!right.vector_on_left);
    check!(right.op == ComparisonOp::Greater);
    check!(!right.bool_modifier);

    let left = parse("vector(1) > up").expect("a vector on the left");
    check!(left.metric_query == "up", "the metric is still the metric");
    check!(left.vector_query == "vector(1)");
    check!(left.vector_on_left, "but the side is recorded");
    check!(left.op == ComparisonOp::Greater);

    // The `bool` modifier is stripped from the right and remembered.
    let modified = parse("up > bool vector(1)").expect("bool is allowed");
    check!(modified.bool_modifier);
    check!(
        modified.vector_query == "vector(1)",
        "bool is not part of the query"
    );
    check!(modified.metric_query == "up");

    // Every comparison operator reaches the expression.
    for (query, op) in [
        ("up == vector(1)", ComparisonOp::Equal),
        ("up != vector(1)", ComparisonOp::NotEqual),
        ("up < vector(1)", ComparisonOp::Less),
        ("up <= vector(1)", ComparisonOp::LessEqual),
        ("up >= vector(1)", ComparisonOp::GreaterEqual),
    ] {
        check!(parse(query).expect("parses").op == op, "{query}");
    }

    // Two vectors, or none, is not this kind of expression.
    check!(parse("vector(1) > vector(2)").is_none(), "two literals");
    check!(parse("up > down").is_none(), "no literal");
    check!(parse("up").is_none(), "no comparison at all");
    check!(parse("").is_none());
}
