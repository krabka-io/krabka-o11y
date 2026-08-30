use super::*;

/// `prometheus_duration_unit` maps a unit to its ordinal, its bit, and how
/// many nanoseconds it is worth. The ordinals and bits are checked as for
/// `detected_duration_unit`; the nanoseconds are checked against each
/// other rather than restated, because a wrong power of ten in a column of
/// long literals is invisible read straight and obvious as a ratio.
#[test]
pub(crate) fn duration_units_are_worth_what_they_should_relative_to_each_other() {
    let ns = |name: &str| {
        let (_, _, nanos) = super::super::prelude::prometheus_duration_unit(name).expect("known unit");
        nanos
    };

    check!(ns("ns") == 1, "the base unit");
    check!(ns("us") == 1_000 * ns("ns"));
    check!(ns("ms") == 1_000 * ns("us"));
    check!(ns("s") == 1_000 * ns("ms"));
    check!(ns("m") == 60 * ns("s"));
    check!(ns("h") == 60 * ns("m"));
    check!(ns("d") == 24 * ns("h"));
    check!(ns("w") == 7 * ns("d"));
    check!(
        ns("y") == 365 * ns("d"),
        "a year here is 365 days, not 52 weeks"
    );

    // The ordinal and bit columns carry the same contract as the detected
    // table, so they get the same check.
    for (name, ordinal) in [
        ("y", 0_u8),
        ("w", 1),
        ("d", 2),
        ("h", 3),
        ("m", 4),
        ("s", 5),
        ("ms", 6),
        ("us", 7),
        ("ns", 8),
    ] {
        let (got, bit, _) = super::super::prelude::prometheus_duration_unit(name).expect("known unit");
        check!(got == ordinal, "{name} ordinal");
        check!(bit == 1_u16 << ordinal, "{name} bit");
    }

    check!(super::prelude::prometheus_duration_unit("") == None);
    check!(super::prelude::prometheus_duration_unit("mo") == None);
    check!(
        super::prelude::prometheus_duration_unit("S") == None,
        "case-sensitive"
    );
}
