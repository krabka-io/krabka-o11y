use super::*;

/// `is_prometheus_duration_literal` accepts "1h30m" and refuses "30m1h":
/// the units must run strictly from larger to smaller, which is what makes
/// a duration unambiguous without needing to add the parts up. A repeat is
/// refused by the same rule, since a unit is never strictly larger than
/// itself -- which is why the ordering test is `<=` and not `<`.
#[test]
pub(crate) fn a_prometheus_duration_literal_runs_from_larger_units_to_smaller() {
    let is_duration = super::super::prelude::is_prometheus_duration_literal;

    // Every unit, in the one order that is allowed.
    check!(is_duration("1y2w3d4h5m6s7ms8us9ns"));
    for unit in ["y", "w", "d", "h", "m", "s", "ms", "us", "ns"] {
        check!(is_duration(&format!("1{unit}")), "{unit} alone");
    }
    check!(is_duration("1h30m"));
    check!(is_duration("90s"));
    check!(is_duration("0s"), "zero is a duration");

    // Out of order, in both the obvious and the subtle spelling. "1ms1m"
    // is the subtle one: read as text it looks ascending, but ms is the
    // SMALLER unit and so may not come first.
    check!(!is_duration("30m1h"));
    check!(!is_duration("1s1h"));
    check!(!is_duration("1ms1m"));
    check!(is_duration("1m1ms"), "that pair the right way round");
    check!(is_duration("1s1ms"), "and seconds before milliseconds");

    // A repeated unit, adjacent or separated.
    check!(!is_duration("1h1h"));
    check!(!is_duration("1h1m1h"));

    // Every chunk needs both a count and a unit.
    check!(!is_duration(""), "nothing is not a duration");
    check!(!is_duration("1"), "a bare number has no unit");
    check!(!is_duration("h"), "a bare unit has no count");
    check!(!is_duration("1h30"), "the trailing chunk has no unit");

    // Unknown units, and units that are only a prefix of a real one.
    check!(!is_duration("1x"));
    check!(!is_duration("1hh"));
    check!(!is_duration("1sec"));

    // Nothing else is allowed between chunks: no sign, no point, no space.
    check!(!is_duration("1.5h"));
    check!(!is_duration("-1h"));
    check!(!is_duration("1h "));
    check!(!is_duration("1h 30m"));
}
