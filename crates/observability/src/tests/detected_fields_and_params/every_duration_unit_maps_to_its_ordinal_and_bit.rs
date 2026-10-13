use super::*;

/// Both duration-unit tables use one ordinal/bit contract; the Prometheus
/// table also exposes each unit's nanosecond value.
#[yare::parameterized(
    year = { "y", 0_u8, 31_536_000_000_000_000_i128 },
    week = { "w", 1_u8, 604_800_000_000_000_i128 },
    day = { "d", 2_u8, 86_400_000_000_000_i128 },
    hour = { "h", 3_u8, 3_600_000_000_000_i128 },
    minute = { "m", 4_u8, 60_000_000_000_i128 },
    second = { "s", 5_u8, 1_000_000_000_i128 },
    millisecond = { "ms", 6_u8, 1_000_000_i128 },
    microsecond = { "us", 7_u8, 1_000_i128 },
    nanosecond = { "ns", 8_u8, 1_i128 },
)]
fn duration_unit_maps_to_ordinal_bit_and_nanoseconds(name: &str, ordinal: u8, nanoseconds: i128) {
    let expected = crate::querier::analytics::detected_fields::DetectedDurationUnit {
        order: ordinal,
        bit: 1_u16 << ordinal,
    };
    check!(crate::tests::prelude::detected_duration_unit(name) == Some(expected));
    let prometheus = crate::tests::prelude::prometheus_duration_unit(name).unwrap();
    check!(prometheus.rank == ordinal);
    check!(prometheus.bit == expected.bit);
    check!(prometheus.nanoseconds == nanoseconds);
}

#[test]
fn duration_unit_tables_reject_unknown_and_case_changed_units() {
    for unit in ["", "Y", "mo", "sec", "S"] {
        check!(crate::tests::prelude::detected_duration_unit(unit) == None);
        check!(crate::tests::prelude::prometheus_duration_unit(unit) == None);
    }
}
