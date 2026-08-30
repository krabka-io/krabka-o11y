use super::*;

/// `detected_duration_unit` maps a unit to its ordinal and its bit. Both
/// come from the same table, and a table is exactly where an off-by-one
/// goes unnoticed, so every entry is checked rather than sampled -- and
/// the bit is checked against the ordinal it is meant to shadow.
#[test]
pub(crate) fn every_duration_unit_maps_to_its_ordinal_and_bit() {
    let unit = super::super::prelude::detected_duration_unit;

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
        let expected = (ordinal, 1_u16 << ordinal);
        check!(unit(name) == Some(expected), "{name}");
    }

    // The bits are distinct, which is what makes them usable as a set.
    let mut seen = 0_u16;
    for name in ["y", "w", "d", "h", "m", "s", "ms", "us", "ns"] {
        let (_, bit) = unit(name).expect("known unit");
        check!(seen & bit == 0, "{name} reuses a bit");
        seen |= bit;
    }

    check!(unit("") == None);
    check!(unit("Y") == None, "the match is case-sensitive");
    check!(unit("mo") == None, "months are not a unit here");
    check!(unit("sec") == None);
}
