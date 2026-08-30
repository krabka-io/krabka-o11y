use super::*;

/// `parse_decimal_seconds_timestamp` reads "seconds.fraction" as
/// nanoseconds. It REQUIRES the point -- a bare integer is handled
/// elsewhere, as seconds or as nanos depending on context, and guessing
/// here would pre-empt that. The fraction is padded to nine places and
/// truncated past them, so a microsecond timestamp scales correctly.
///
/// The `take(9)` bounding that loop is belt-and-braces: the scale divides
/// by ten each digit and reaches zero by integer division after the ninth,
/// so a tenth digit contributes nothing whether it is read or not.
/// Widening the take is an equivalent mutation.
#[test]
pub(crate) fn a_decimal_seconds_timestamp_scales_its_fraction_to_nanos() {
    let parse = super::super::prelude::parse_decimal_seconds_timestamp;

    // The fraction is positional: one digit is tenths, not nanos.
    check!(parse("5.5") == Some(5_500_000_000));
    check!(parse("5.05") == Some(5_050_000_000));
    check!(parse("0.000000001") == Some(1), "one nanosecond");
    check!(parse("1.000000000") == Some(1_000_000_000));

    // Past nine places the rest is dropped rather than rounded.
    check!(
        parse("0.0000000009") == Some(0),
        "a tenth of a nanosecond is lost"
    );
    check!(parse("1.9999999999") == Some(1_999_999_999));

    // Either side may be empty, but not both.
    check!(parse(".5") == Some(500_000_000));
    check!(parse("5.") == Some(5_000_000_000));
    check!(parse(".").is_none());

    // Signs, including a negative instant.
    check!(parse("-5.5") == Some(-5_500_000_000));
    check!(parse("+5.5") == Some(5_500_000_000));

    // The point is required: a bare integer is somebody else's problem.
    check!(parse("5").is_none(), "no point, no answer");
    check!(parse("").is_none());
    check!(parse("abc").is_none());
    check!(parse("5.abc").is_none());
    check!(parse("5.5.5").is_none(), "the second point is not a digit");
}
