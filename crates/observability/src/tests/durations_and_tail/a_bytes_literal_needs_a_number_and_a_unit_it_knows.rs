use super::*;

/// `is_bytes_literal` accepts "1MB" and "1.5GiB": a non-negative finite
/// number followed by a unit it knows. The split is at the first letter,
/// so the number and the unit are never ambiguous -- and both the decimal
/// and binary spellings of each magnitude are units, since Loki accepts
/// both.
#[test]
pub(crate) fn a_bytes_literal_needs_a_number_and_a_unit_it_knows() {
    let is_bytes = super::super::prelude::is_bytes_literal;

    for unit in [
        "B", "kB", "KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB",
    ] {
        check!(is_bytes(&format!("1{unit}")), "{unit}");
    }
    check!(is_bytes("1.5GiB"), "a fractional amount");
    check!(is_bytes("0B"), "zero bytes is a size");

    // A number with no unit, or a unit with no number.
    check!(!is_bytes("1"));
    check!(!is_bytes(""));
    check!(!is_bytes("MB"), "the amount is empty, which does not parse");

    // Units it does not know, including near-misses.
    check!(!is_bytes("1PB"));
    check!(!is_bytes("1mb"), "the units are case-sensitive");
    check!(!is_bytes("1MBs"));
    check!(!is_bytes("1Mib"));

    // A negative amount is refused rather than clamped to zero.
    check!(!is_bytes("-1MB"));

    // "inf" and "NaN" contain letters, so the split puts them in the UNIT
    // and leaves the amount empty -- they are refused for having no
    // number, not for being non-finite.
    check!(!is_bytes("infMB"));
    check!(!is_bytes("NaNMB"));

    // The finiteness check is reached by a number with no letters in it at
    // all: four hundred digits overflow an f64 to infinity, and a size of
    // infinity is not a size.
    let overflowing = format!("{}MB", "1".repeat(400));
    check!(
        !is_bytes(&overflowing),
        "an amount that overflows to infinity"
    );
}
