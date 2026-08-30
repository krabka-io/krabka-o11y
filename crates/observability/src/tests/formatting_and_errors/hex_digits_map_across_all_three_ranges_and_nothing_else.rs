use super::*;

/// `hex_value` maps a hex digit to its value across three ranges. Every
/// range boundary is checked together with the character immediately
/// outside it, since a range widened or narrowed by one is invisible from
/// the middle -- and the two letter ranges must not be confused, because
/// their offsets differ by the distance between the cases.
#[test]
pub(crate) fn hex_digits_map_across_all_three_ranges_and_nothing_else() {
    let value = super::super::prelude::hex_value;

    check!(value(b'0') == Some(0), "the low edge of the digits");
    check!(value(b'9') == Some(9), "and the high edge");
    check!(value(b'5') == Some(5));
    check!(value(b'a') == Some(10), "lower-case a continues from nine");
    check!(value(b'f') == Some(15));
    check!(value(b'A') == Some(10), "upper-case is the same value");
    check!(value(b'F') == Some(15));

    // One character outside each range, on both sides.
    check!(value(b'/') == None, "just below '0'");
    check!(value(b':') == None, "just above '9'");
    check!(value(b'`') == None, "just below 'a'");
    check!(value(b'g') == None, "just above 'f'");
    check!(value(b'@') == None, "just below 'A'");
    check!(value(b'G') == None, "just above 'F'");

    // The gap between the two letter ranges is not a range.
    check!(value(b'Z') == None);
    check!(value(b' ') == None);
}
