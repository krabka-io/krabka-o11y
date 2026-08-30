use super::*;

/// `parse_decimal_sample_literal` reads a decimal literal as an EXACT
/// rational rather than a float, which is the whole point: 0.1 has no
/// float representation, and a sample that round-trips through one comes
/// back as 0.100000000000000005551. The denominator is the power of ten
/// the fraction needed, so the pair is returned unreduced.
///
/// The exponent shifts that power either way, and the two directions take
/// different branches -- a negative shift multiplies the numerator, a
/// positive one raises the denominator -- so both are checked.
///
/// Two mutations here are equivalent rather than untested. The branch test
/// `decimal_places >= 0` could be `> 0`: at zero both paths raise ten to
/// the zeroth and leave the numerator alone. And the early refusal of a
/// second exponent marker is a fast path only -- `parse_decimal_sample_
/// exponent` calls `parse::<i32>()`, which rejects anything containing an
/// `e` anyway. Both are pinned by behaviour that cannot distinguish them.
#[test]
pub(crate) fn a_decimal_sample_literal_parses_to_an_exact_rational() {
    let parse = super::super::prelude::parse_decimal_sample_literal;

    // Whole numbers and plain decimals, unreduced.
    check!(parse("1") == Some((1, 1)));
    check!(parse("0") == Some((0, 1)));
    check!(parse("1.5") == Some((15, 10)), "unreduced: not (3, 2)");
    check!(parse("0.1") == Some((1, 10)), "exact, where a float is not");
    check!(parse("12.345") == Some((12_345, 1_000)));

    // Signs, on either spelling.
    check!(parse("-1.5") == Some((-15, 10)));
    check!(parse("+1.5") == Some((15, 10)));
    check!(parse("-0") == Some((0, 1)));

    // A missing side of the point is allowed as long as one side is there.
    check!(parse(".5") == Some((5, 10)));
    check!(parse("5.") == Some((5, 1)));
    check!(parse(".").is_none(), "but not both missing");

    // A positive exponent cancels decimal places and can go past them,
    // which switches branches: the numerator is scaled instead.
    check!(parse("1e3") == Some((1_000, 1)));
    check!(parse("1.5e2") == Some((150, 1)), "past the decimal places");
    check!(parse("1.5e1") == Some((15, 1)), "exactly cancelling them");
    check!(
        parse("1.25e1") == Some((125, 10)),
        "partially cancelling them"
    );

    // A negative exponent adds places, raising the denominator.
    check!(parse("1e-3") == Some((1, 1_000)));
    check!(parse("1.5e-2") == Some((15, 1_000)));
    check!(
        parse("15E-1") == Some((15, 10)),
        "the exponent marker is either case"
    );

    // Refusals: nothing to parse, or not a number.
    check!(parse("").is_none());
    check!(parse("-").is_none());
    check!(parse("abc").is_none());
    check!(
        parse("1.2.3").is_none(),
        "a second point is part of the fraction"
    );
    check!(parse("1e2e3").is_none(), "and a second exponent is refused");
    check!(parse("1e").is_none());
    check!(
        parse(" 1").is_none(),
        "no trimming: whitespace is not a digit"
    );
}
