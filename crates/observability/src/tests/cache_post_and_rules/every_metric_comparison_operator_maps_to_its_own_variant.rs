use super::*;

/// The comparison operators are a six-entry table, and every strict one
/// has a non-strict twin one character longer. Checking them entry by
/// entry is what separates the pairs; sampling would not.
#[test]
pub(crate) fn every_metric_comparison_operator_maps_to_its_own_variant() {
    use super::super::prelude::{ComparisonOp, parse_metric_comparison_operator as parse};

    check!(parse("==") == Some(ComparisonOp::Equal));
    check!(parse("!=") == Some(ComparisonOp::NotEqual));
    check!(parse(">") == Some(ComparisonOp::Greater));
    check!(parse(">=") == Some(ComparisonOp::GreaterEqual));
    check!(parse("<") == Some(ComparisonOp::Less));
    check!(parse("<=") == Some(ComparisonOp::LessEqual));

    // Nothing else is an operator, including the near-misses.
    check!(parse("") == None);
    check!(parse("=") == None, "a single equals is not a comparison");
    check!(parse("=>") == None);
    check!(parse("=<") == None);
    check!(parse("<>") == None);
    check!(parse("===") == None);
    check!(parse(">>") == None);
}
