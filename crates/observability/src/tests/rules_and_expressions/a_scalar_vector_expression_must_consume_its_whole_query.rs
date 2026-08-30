use super::*;

/// `scalar_vector_expression_result` evaluates the scalar-and-vector
/// sub-language: arithmetic over numbers, and `vector(...)` producing a
/// series. Two things about it are easy to get wrong and are pinned here.
///
/// First, whitespace is stripped BEFORE parsing rather than skipped during
/// it, so "1 + 1" and "1+1" are the same expression -- and so, less
/// happily, are "1 1" and "11". Second, the parser
/// must be FINISHED: "1+1x" is refused rather than evaluated as "1+1" with
/// the tail ignored, which would silently accept a typo as a valid query.
#[test]
pub(crate) fn a_scalar_vector_expression_must_consume_its_whole_query() {
    use super::super::prelude::ScalarVectorExpressionResult;

    let result = super::super::prelude::scalar_vector_expression_result;
    let scalar = |query: &str| match result(query) {
        Some(ScalarVectorExpressionResult::Scalar { sample }) => Some(sample),
        _ => None,
    };

    // Plain arithmetic, with and without spaces.
    check!(scalar("1").as_deref() == Some("1"));
    check!(scalar("1+1").as_deref() == Some("2"));
    check!(
        scalar("1 + 1").as_deref() == Some("2"),
        "whitespace is stripped first"
    );
    check!(scalar("  2 * 3  ").as_deref() == Some("6"));
    check!(
        scalar("(1+2)*3").as_deref() == Some("9"),
        "parentheses group"
    );

    // A vector literal is the other result shape.
    check!(matches!(
        result("vector(1)"),
        Some(ScalarVectorExpressionResult::Vector { .. })
    ));
    check!(
        matches!(
            result("vector( 1 )"),
            Some(ScalarVectorExpressionResult::Vector { .. }),
        ),
        "whitespace inside the call too"
    );

    // Trailing junk is refused rather than ignored. This is the case that
    // the `is_finished` check exists for: without it "1+1x" evaluates to 2
    // and a typo becomes a valid query.
    check!(result("1+1x").is_none());
    check!(result("vector(1)x").is_none());
    // But "1 1" is not junk -- stripping whitespace FIRST makes it the
    // single number eleven. That follows from the strip being a rewrite of
    // the input rather than a skip during parsing, and it is pinned
    // because it is surprising, not because it is desirable.
    check!(scalar("1 1").as_deref() == Some("11"));

    // A set operator needs a vector on BOTH sides. Each of the two counts
    // is a strict increase over the terms seen before that side was
    // parsed, and "at least as many" is trivially true -- so a side with
    // no vector at all is the only thing that separates them.
    check!(matches!(
        result("vector(1) and vector(2)"),
        Some(ScalarVectorExpressionResult::Vector { .. })
    ));
    check!(result("1 and vector(1)").is_none(), "no vector on the left");
    check!(result("vector(1) and 1").is_none(), "none on the right");
    check!(result("1 and 1").is_none(), "none on either side");

    // A comparison carrying `on(...)`/`ignoring(...)` needs a vector on
    // both sides too. Without a modifier the same comparison is fine, so
    // the modifier is what turns the requirement on.
    check!(
        result("vector(1) > 0").is_some(),
        "no modifier, no requirement"
    );
    check!(matches!(
        result("vector(1) > on() vector(2)"),
        Some(ScalarVectorExpressionResult::Vector { .. })
    ));
    check!(
        result("1 > on() vector(1)").is_none(),
        "a modifier with no vector on the left"
    );
    check!(
        result("vector(1) > ignoring() 1").is_none(),
        "and none on the right"
    );

    // An escape inside a string literal is decoded, and the parser advances
    // PAST it. Every other string here is escape-free, where advancing the
    // wrong way would go unnoticed.
    let replaced = result(r#"label_replace(vector(1),"dst","a\nb","src","(.*)")"#);
    let Some(ScalarVectorExpressionResult::Vector { metric, .. }) = replaced else {
        panic!("expected a vector result");
    };
    check!(
        metric.get("dst").map(String::as_str) == Some("a\nb"),
        "got {metric:?}"
    );

    // Not this sub-language at all.
    check!(result("up").is_none());
    check!(result("").is_none());
    check!(result("+").is_none());
    check!(result("(1").is_none(), "an unclosed group is not finished");
}
