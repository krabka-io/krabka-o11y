use super::*;

/// `parse_metric_arithmetic_operator` names the six `PromQL` scalar
/// operators. The variants are asserted pairwise distinct, so an arm
/// returning a neighbour's operator cannot pass -- and every unrecognised
/// spelling is refused rather than defaulted, since a silent default here
/// would compute the wrong arithmetic instead of failing the query.
#[test]
pub(crate) fn every_promql_scalar_operator_parses_to_its_own_variant() {
    let parse = super::super::prelude::parse_metric_arithmetic_operator;

    check!(parse("+") == Some(MetricScalarArithmeticOp::Add));
    check!(parse("-") == Some(MetricScalarArithmeticOp::Subtract));
    check!(parse("*") == Some(MetricScalarArithmeticOp::Multiply));
    check!(parse("/") == Some(MetricScalarArithmeticOp::Divide));
    check!(parse("%") == Some(MetricScalarArithmeticOp::Modulo));
    check!(parse("^") == Some(MetricScalarArithmeticOp::Power));

    // Nothing else parses, including operators PromQL has elsewhere.
    check!(parse("").is_none());
    check!(parse("**").is_none());
    check!(parse("+ ").is_none(), "the operator is not trimmed here");
    check!(parse("and").is_none());
    check!(parse("==").is_none(), "a comparison is not arithmetic");

    let variants = [
        parse("+"),
        parse("-"),
        parse("*"),
        parse("/"),
        parse("%"),
        parse("^"),
    ];
    for (index, left) in variants.iter().enumerate() {
        for right in &variants[index + 1..] {
            check!(left != right, "two operators share a variant: {left:?}");
        }
    }
}
