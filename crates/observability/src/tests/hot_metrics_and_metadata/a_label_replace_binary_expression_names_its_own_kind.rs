use super::*;

/// `parse_label_replace_metric_binary_expression` recognises a binary
/// expression where EITHER side is a `label_replace(...)`, and reports
/// which kind of binary it is. The three kinds are tried in order --
/// arithmetic, comparison, set -- and each must produce its own variant,
/// since they are handled by different evaluators downstream.
///
/// Either side qualifying is the point: a `label_replace` on the right
/// alone is just as much this shape as one on the left, and the two go
/// through the same `||`.
#[test]
pub(crate) fn a_label_replace_binary_expression_names_its_own_kind() {
    use super::super::prelude::LabelReplaceMetricBinaryExpression as Expression;

    let parse = super::super::prelude::parse_label_replace_metric_binary_expression;
    let replace = r#"label_replace(up,"a","b","c","d")"#;

    // Arithmetic, with the label_replace on each side in turn.
    check!(matches!(
        parse(&format!("{replace} + up")),
        Some(Expression::Arithmetic { .. })
    ));
    check!(
        matches!(
            parse(&format!("up + {replace}")),
            Some(Expression::Arithmetic { .. })
        ),
        "on the right is equally this shape"
    );

    // Comparison and set each get their own variant.
    check!(matches!(
        parse(&format!("{replace} > up")),
        Some(Expression::Comparison { .. })
    ));
    check!(matches!(
        parse(&format!("{replace} and up")),
        Some(Expression::Set { .. })
    ));

    // The operands are carried through trimmed, not with the whitespace
    // the split left on them.
    let Some(Expression::Arithmetic { left, right, .. }) = parse(&format!("{replace}  +  up"))
    else {
        panic!("an arithmetic expression");
    };
    check!(left == replace, "the left operand is trimmed");
    check!(right == "up", "and so is the right");

    // The operator is carried through, not assumed. Subtraction is used
    // because it is not the variant a collapsed arm would default to.
    let Some(Expression::Arithmetic { op, .. }) = parse(&format!("{replace} - up")) else {
        panic!("an arithmetic expression");
    };
    check!(op == krabka_logql::MetricScalarArithmeticOp::Subtract);
    let Some(Expression::Comparison { op, .. }) = parse(&format!("{replace} < up")) else {
        panic!("a comparison expression");
    };
    check!(op == krabka_logql::ComparisonOp::Less);

    // A binary expression with no label_replace on either side is not this
    // shape, and is parsed elsewhere.
    check!(parse("up + down").is_none());
    check!(parse("up > down").is_none());
    check!(parse("up and down").is_none());

    // Nor is a bare label_replace with no binary operator at all.
    check!(parse(replace).is_none());
    check!(parse("").is_none());
}
