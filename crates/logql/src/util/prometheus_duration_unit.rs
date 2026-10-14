/// Looks up one Prometheus duration unit.
///
/// Returns the unit's rank from largest (`y`, 0) to smallest (`ns`, 8), a bit
/// that is unique to the unit, and the unit's length in nanoseconds. A year is
/// 365 days, as in Prometheus `model.ParseDuration`. Units are case-sensitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrometheusDurationUnit {
    pub rank: u8,
    pub bit: u16,
    pub nanoseconds: i128,
}

#[must_use]
pub fn prometheus_duration_unit(unit: &str) -> Option<PrometheusDurationUnit> {
    match unit {
        "y" => Some(PrometheusDurationUnit {
            rank: 0,
            bit: 0x001,
            nanoseconds: 31_536_000_000_000_000,
        }),
        "w" => Some(PrometheusDurationUnit {
            rank: 1,
            bit: 0x002,
            nanoseconds: 604_800_000_000_000,
        }),
        "d" => Some(PrometheusDurationUnit {
            rank: 2,
            bit: 0x004,
            nanoseconds: 86_400_000_000_000,
        }),
        "h" => Some(PrometheusDurationUnit {
            rank: 3,
            bit: 0x008,
            nanoseconds: 3_600_000_000_000,
        }),
        "m" => Some(PrometheusDurationUnit {
            rank: 4,
            bit: 0x010,
            nanoseconds: 60_000_000_000,
        }),
        "s" => Some(PrometheusDurationUnit {
            rank: 5,
            bit: 0x020,
            nanoseconds: 1_000_000_000,
        }),
        "ms" => Some(PrometheusDurationUnit {
            rank: 6,
            bit: 0x040,
            nanoseconds: 1_000_000,
        }),
        "us" => Some(PrometheusDurationUnit {
            rank: 7,
            bit: 0x080,
            nanoseconds: 1_000,
        }),
        "ns" => Some(PrometheusDurationUnit {
            rank: 8,
            bit: 0x100,
            nanoseconds: 1,
        }),
        _ => None,
    }
}
