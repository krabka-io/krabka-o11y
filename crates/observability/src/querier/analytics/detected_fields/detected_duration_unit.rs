#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetectedDurationUnit {
    pub(crate) order: u8,
    pub(crate) bit: u16,
}

pub(crate) fn detected_duration_unit(unit: &str) -> Option<DetectedDurationUnit> {
    match unit {
        "y" => Some(DetectedDurationUnit {
            order: 0,
            bit: 1 << 0,
        }),
        "w" => Some(DetectedDurationUnit {
            order: 1,
            bit: 1 << 1,
        }),
        "d" => Some(DetectedDurationUnit {
            order: 2,
            bit: 1 << 2,
        }),
        "h" => Some(DetectedDurationUnit {
            order: 3,
            bit: 1 << 3,
        }),
        "m" => Some(DetectedDurationUnit {
            order: 4,
            bit: 1 << 4,
        }),
        "s" => Some(DetectedDurationUnit {
            order: 5,
            bit: 1 << 5,
        }),
        "ms" => Some(DetectedDurationUnit {
            order: 6,
            bit: 1 << 6,
        }),
        "us" => Some(DetectedDurationUnit {
            order: 7,
            bit: 1 << 7,
        }),
        "ns" => Some(DetectedDurationUnit {
            order: 8,
            bit: 1 << 8,
        }),
        _ => None,
    }
}
