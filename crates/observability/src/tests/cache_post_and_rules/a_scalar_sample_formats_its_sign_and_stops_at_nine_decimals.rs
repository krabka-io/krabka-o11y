use super::*;

/// A scalar renders its sign from the numerator alone, and a decimal that
/// does not terminate stops at nine digits. Zero is not negative, which is
/// what separates `< 0` from `<= 0`; a negative that is not zero separates
/// it from `== 0`; and a repeating decimal is the only input that reaches
/// the digit cap at all.
#[test]
pub(crate) fn a_scalar_sample_formats_its_sign_and_stops_at_nine_decimals() {
    let format = |numerator: i128, denominator: u128| {
        super::super::prelude::ScalarSample::new(numerator, denominator).format()
    };

    check!(format(0, 1) == "0", "zero carries no sign");
    check!(format(7, 1) == "7");
    check!(format(-7, 1) == "-7");
    check!(format(3, 2) == "1.5");
    check!(format(-3, 2) == "-1.5");
    check!(format(1, 8) == "0.125");

    // Truncated at nine digits, not rounded and not run on.
    check!(format(1, 3) == "0.333333333");
    check!(format(-2, 3) == "-0.666666666");
}
