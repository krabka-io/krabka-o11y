use super::*;

/// `parse_decimal_seconds_timestamp` reads seconds with a fractional part
/// into whole nanoseconds. The fraction is positional -- the first digit
/// is tenths, not units -- so a scale applied the wrong way round is the
/// mistake worth catching, and it only shows on a fraction shorter than
/// nine digits.
#[test]
pub(crate) fn decimal_second_timestamps_scale_their_fraction_by_position() {
    let parse = super::super::prelude::parse_decimal_seconds_timestamp;

    check!(parse("0.0") == Some(0));
    check!(parse("1.0") == Some(1_000_000_000));
    check!(
        parse("1.5") == Some(1_500_000_000),
        "one digit is tenths, not units"
    );
    check!(parse("0.5") == Some(500_000_000));
    check!(
        parse("0.05") == Some(50_000_000),
        "the second digit is hundredths"
    );
    check!(
        parse("0.000000001") == Some(1),
        "nine digits reach nanoseconds"
    );

    // Past nine digits the rest is dropped rather than overflowing the
    // scale into zero or below.
    check!(
        parse("0.0000000019") == Some(1),
        "the tenth digit is ignored"
    );

    // Signs, on both sides of zero.
    check!(parse("-1.5") == Some(-1_500_000_000));
    check!(
        parse("+1.5") == Some(1_500_000_000),
        "an explicit plus is allowed"
    );
    check!(parse("-0.0") == Some(0));

    // A missing part on either side of the point is still a number.
    check!(parse("1.") == Some(1_000_000_000), "no fraction");
    check!(parse(".5") == Some(500_000_000), "no whole part");

    // What is not a decimal at all.
    check!(parse("1") == None, "a point is required");
    check!(parse(".") == None, "and digits on one side of it");
    check!(parse("") == None);
    check!(parse("a.b") == None);
    check!(parse("1.5x") == None, "trailing text is not a fraction");
}
